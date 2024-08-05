# -----------------------------
__file__ = 'pp_df.py'
__doc__ = """
run only 1 pass, no parallel compute
1 data reader, 1 merge, 1 mcc, for each direction, and 1 reconciliation in total
"""
# -----------------------------

import os
import json
import time

# Custom modules
import data_feed as df
import min_cost_flow as mcf
import reconciliation as rec
import merge
from stream_manager import StreamManager, StreamSeries

import i24_logger.log_writer as log_writer


logger = log_writer.logger
logger.set_name('master')
setattr(logger, "_default_logger_extra",  {})


def main(raw_collection = None, reconciled_collection = None):
    with open('ppdf.txt', 'w') as f:
        f.write('test')
    _start = time.time()

    # %% Parameters, data structures and processes
    # GET PARAMAETERS
    with open("parameters.json") as f:
        parameters = json.load(f)
    
    if raw_collection:
        parameters["raw_collection"] = raw_collection
    
    if reconciled_collection:
        parameters["reconciled_collection"] = reconciled_collection
    
    db_param = None
    
    # CREATE A MANAGER
    # mp_manager = mp.Manager()
    sm = StreamManager()
    print("Post-processing manager has PID={}".format(os.getpid()))

    # SHARED DATA STRUCTURES
    # sm.param = mp_manager.dict()
    sm.param.update(parameters)
    sm.param["time_win"] = sm.param["master_time_win"]
    sm.param["stitcher_args"]["stitch_thresh"] = sm.param["stitcher_args"]["master_stitch_thresh"]
    # sm.param["stitcher_mode"] = "master" # switch from local to master
    
    # initialize some db collections
    print("Post-processing manager initialized db collections. Creating shared data structures")
    master_stitch: list[StreamSeries] = []
    for dir in ("eb", "wb"):
        
        # feed
        print('feed', dir)
        key1 = "master_"+dir+"_feed"
        query_filter = {"direction": 1 if dir == "eb" else -1}
        master_feed = sm.pipe(df.static_data_reader, 2)(sm.param, db_param, query_filter, name=key1)
        
        # merge
        print('merge', dir)
        key2 =  "master_"+dir+"_merge"
        master_merge = sm.pipe(merge.merge_fragments, 2)(dir, master_feed, sm.param, key2, name=key2)
        
        # stitch
        print('stitch', dir)
        key3 = "master_"+dir+"_stitch"
        master_stitch.append(sm.pipe(mcf.min_cost_flow_online_alt_path, 2)(dir, master_merge, parameters, key3, name=key3))
    
    print('merge stitched trajectories from east bound and west bound')
    merged_master_stiched = sm.merge_queues(*master_stitch)

    print('reconcile')
    master_reconcile = sm.pipe(rec.reconciliation_pool, 3)(sm.param, db_param, merged_master_stiched, name="master_reconcile")

    print('write reconcile to db')
    sm.pipe(rec.write_reconciled_to_db)(sm.param, db_param, master_reconcile, name="write_reconciled_to_db")

    print('Done')
    sm.keep_alive()

    with open('ppdf.txt', 'w') as f:
        f.write(f'{time.time() - _start}')


if __name__ == '__main__':
    main()
