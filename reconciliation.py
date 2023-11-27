# -----------------------------
__file__ = 'reconciliation.py'
__doc__ = """

Update 6/27/2021
If each worker handles mongodb client connection, then they have to close the connection in order for the pool to join (in the event of graceful shutdown).
It is not recommended by mongodb to open and close connections all the time.
Instead, modify the code such that open the connection for dbreader and writer at the parent process. Each worker does not have direct access to mongodb client.
After done processing, each worker send results back to the parent process using a queue
"""

# -----------------------------
import multiprocessing
from multiprocessing import Pool
import time
import os
import queue
import i24_logger.log_writer as log_writer
from decimal import Decimal
import json

# from utils.utils_reconciliation import receding_horizon_2d_l1, resample, receding_horizon_2d, combine_fragments, rectify_2d
from utils.utils_opt import combine_fragments, resample, opt1, opt2, opt1_l1, opt2_l1, opt2_l1_constr
    

# Custom JSON encoder to handle Decimal objects
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return str(o)  # Convert Decimal to a string representation
        return super().default(o)
    
    
def reconcile_single_trajectory(reconciliation_args, combined_trajectory, reconciled_queue) -> None:
    """
    Resample and reconcile a single trajectory, and write the result to a queue
    :param next_to_reconcile: a trajectory document
    :return:
    """
    
    rec_worker_logger = log_writer.logger 
    rec_worker_logger.set_name("rec_worker")
    setattr(rec_worker_logger, "_default_logger_extra",  {})

    resampled_trajectory = resample(combined_trajectory, dt=0.04)
    if "post_flag" in resampled_trajectory:
        # skip reconciliation
        rec_worker_logger.info("+++ Flag as low conf, skip reconciliation", extra = None)

    else:
        try:
            # finished_trajectory = rectify_2d(resampled_trajectory, reg = "l1", **reconciliation_args)  
            finished_trajectory = opt2_l1_constr(resampled_trajectory, **reconciliation_args)  
            # finished_trajectory = opt2(resampled_trajectory, **reconciliation_args)  
            reconciled_queue.put(finished_trajectory)
            # rec_worker_logger.debug("*** Reconciled a trajectory, duration: {:.2f}s, length: {}".format(finished_trajectory["last_timestamp"]-finished_trajectory["first_timestamp"], len(finished_trajectory["timestamp"])), extra = None)
        
        except Exception as e:
            rec_worker_logger.info("+++ Flag as {}, skip reconciliation".format(str(e)), extra = None)



def reconciliation_pool(parameters, db_param, stitched_trajectory_queue: multiprocessing.Queue, 
                        reconciled_queue: multiprocessing.Queue, ) -> None:
    """
    Start a multiprocessing pool, each worker 
    :param stitched_trajectory_queue: results from stitchers, shared by mp.manager
    :param pid_tracker: a dictionary
    :return:
    """

    n_proc = min(multiprocessing.cpu_count(), parameters["worker_size"])
    worker_pool = Pool(processes= n_proc)

    
    # parameters
    reconciliation_args=parameters["reconciliation_args"]
    
    rec_parent_logger = log_writer.logger
    rec_parent_logger.set_name("reconciliation")
    setattr(rec_parent_logger, "_default_logger_extra",  {})

    # wait to get raw collection name
    while parameters["raw_collection"]=="":
        time.sleep(1)
    
    rec_parent_logger.info("** Reconciliation pool starts. Pool size: {}".format(n_proc), extra = None)
    TIMEOUT = parameters["reconciliation_pool_timeout"]
    
    cntr = 0
    while True:
        try:
            try:
                traj_docs = stitched_trajectory_queue.get(timeout = TIMEOUT) #20sec
                cntr += 1
            except queue.Empty: 
                rec_parent_logger.warning("Reconciliation pool is timed out after {}s. Close the reconciliation pool.".format(TIMEOUT))
                worker_pool.close()
                break
            if isinstance(traj_docs, list):
                combined_trajectory = combine_fragments(traj_docs)
            else:
                combined_trajectory = combine_fragments([traj_docs])
            # combined_trajectory = combine_fragments(traj_docs)  
            worker_pool.apply_async(reconcile_single_trajectory, (reconciliation_args, combined_trajectory, reconciled_queue, ))

        except Exception as e: # other exception
            rec_parent_logger.warning("{}, Close the pool".format(e))
            worker_pool.close() # wait until all processes finish their task
            break
            
            
        
    # Finish up  
    worker_pool.join()
    rec_parent_logger.info("Joined the pool.")
    
    return



def write_reconciled_to_db(parameters, db_param, reconciled_queue):
    
    reconciled_writer = log_writer.logger
    reconciled_writer.set_name("reconciliation_writer")
    
    TIMEOUT = parameters["reconciliation_writer_timeout"]
    cntr = 0
    HB = parameters["log_heartbeat"]
    begin = time.time()

    # in case of restart, remove the last "]" in json
    output_filename = parameters["reconciled_collection"]+".json"
    file_exists = os.path.exists(output_filename)
    append_flag = file_exists and os.stat(output_filename).st_size > 0
    
    if append_flag:
        with open(output_filename, 'rb+') as fh:
            fh.seek(-1, os.SEEK_END)
            fh.truncate()
        print("removed last character")

    # Write to db
    while True:

        try:
            record = reconciled_queue.get(timeout = TIMEOUT)
        except queue.Empty:
            reconciled_writer.warning("Getting from reconciled_queue reaches timeout {} sec.".format(TIMEOUT))
            break

        # TODO: write one
        file_exists = os.path.exists(output_filename)
        append_flag = file_exists and os.stat(output_filename).st_size > 0

        # Open the output file for writing or appending
        with open(output_filename, 'a' if file_exists else 'w') as output_file:
            # If the file doesn't exist or is empty, write the start of the JSON array
            if not append_flag:
                output_file.write("[")

            # Check if the file already had data and adjust the comma if needed
            first_item = True if not file_exists or os.stat(output_filename).st_size == 0 else False

            if not first_item:
                output_file.write(",")  # Separate items with commas except for the first one
            else:
                first_item = False
            
            # Write each record (dictionary) to the output file using the custom encoder
            json.dump(record, output_file, cls=DecimalEncoder)
            cntr += 1
            output_file.write("]")  # Close the JSON array
        

        if time.time()-begin > HB:
            begin = time.time()
            
            # TODO: progress update
            reconciled_writer.info(f"Writing {cntr} documents in this batch")
            

    
    # Safely close the mongodb client connection
    reconciled_writer.warning(f"JSON writer closed. Current count: {cntr}. Exit")
    return



    
    
    
    
    
    

if __name__ == '__main__':
    print("not implemented")