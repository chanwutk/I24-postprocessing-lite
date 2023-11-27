#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May 15 15:17:59 2022

@author: yanbing_wang
"""
import queue
import time

import i24_logger.log_writer as log_writer
from utils.utils_mcf import MOTGraphSingle
from utils.misc import calc_fit, find_overlap_idx
from utils.utils_opt import combine_fragments, resample
# import multiprocessing
# import _pickle as pickle
   
    
def min_cost_flow_online_alt_path(direction, fragment_queue, stitched_trajectory_queue, parameters, name=None):
    '''
    incrementally fixing the matching
    '''
    
    # Initiate a logger
    stitcher_logger = log_writer.logger
    if name:
        stitcher_logger.set_name(name)
    else:
        stitcher_logger.set_name("stitcher_"+direction)
    stitcher_logger.info("** min_cost_flow_online_alt_path starts", extra = None)
    # setattr(stitcher_logger, "_default_logger_extra",  {})

    # Get parameters
    ATTR_NAME = parameters["fragment_attr_name"]
    TIME_WIN = parameters["time_win"]
    
    # Initialize tracking graph
    m = MOTGraphSingle(direction=direction, attr=ATTR_NAME, parameters=parameters)
    
    GET_TIMEOUT = parameters["stitcher_timeout"]
    HB = parameters["log_heartbeat"]
    begin = time.time()
    input_obj = 0
    output_obj = 0
    
    while True:
        try:
            try:
                fgmt = fragment_queue.get(timeout = GET_TIMEOUT) # a merged dictionary
                
            except queue.Empty: # queue is empty
                stitcher_logger.info("Getting from fragment_queue timed out after {} sec.".format(GET_TIMEOUT))
                all_paths = m.get_all_traj()
                
                for path in all_paths:
#                     print("queue empty path", path)
                    trajs = m.get_traj_dicts(path)
                    stitched_trajectory_queue.put(trajs[::-1])
                    input_obj += len(path)
                    output_obj += 1
#                     stitcher_logger.info("final stitch together {}".format([trj for trj in path]))
                
                # stitcher_logger.info("fragment_queue is empty, exit.")
                stitcher_logger.info("Final flushing {} raw fragments --> {} stitched fragments".format(input_obj, output_obj),extra = None)
                break

            fgmt_id = fgmt[ATTR_NAME]
            
            # t1 = time.time()
            # ============ Add node ============
            m.add_node(fgmt)
            stitcher_logger.debug("add_node {}".format(fgmt_id))
            # cum_t1 += time.time()-t1
            
            # ============ Path augment ============
            # t2 = time.time()
            m.augment_path(fgmt_id)
            stitcher_logger.debug("augment_path {}".format(fgmt_id))
            # cum_t2 += time.time()-t2
            
            # ============ Pop path ============
            # t3 = time.time()
            all_paths = m.pop_path(time_thresh = fgmt["first_timestamp"] - TIME_WIN)  
            stitcher_logger.debug("all_paths {}".format(len(all_paths) if type(all_paths) is list else []))
            
            num_cache = len(m.cache)
            num_nodes = m.G.number_of_nodes()
            
            for path in all_paths:
                # print("pop path", path)
                trajs = m.get_traj_dicts(path)
                stitched_trajectory_queue.put(trajs[::-1])
                m.clean_graph(path)
#                 stitcher_logger.info("stitch together {}".format([trj for trj in path]))
                
                input_obj += len(path)
                output_obj += 1
                
            stitcher_logger.debug("clean_path cache {}->{}, nodes {}->{}".format(num_cache, len(m.cache),
                                                                                 num_nodes, m.G.number_of_nodes()))
            # cum_t3 += time.time()-t3
            
            # heartbeat log
            now = time.time()
            if now - begin > HB:
                stitcher_logger.info("MCF graph # nodes: {}, # edges: {}, deque: {}, cache: {}".format(m.G.number_of_nodes(), m.G.number_of_edges(), len(m.in_graph_deque), len(m.cache)),extra = None)
                # stitcher_logger.info("Elapsed add:{:.2f}, augment:{:.2f}, pop:{:.2f}, total:{:.2f}".format(cum_t1, cum_t2, cum_t3, now-start), extra=None)
                stitcher_logger.info("{} raw fragments --> {} stitched fragments".format(input_obj, output_obj),extra = None)
                begin = time.time()
            
        except (ConnectionResetError, BrokenPipeError, EOFError) as e:   
            stitcher_logger.warning("Connection error: {}".format(str(e)))
            break
            
        except Exception as e: # other unknown exceptions are handled as error TODO UNTESTED CODE!
            stitcher_logger.error("Other error: {}, push all processed trajs to queue".format(e))
            
            all_paths = m.get_all_traj()
            for path in all_paths:
                # print("exception path", path)
                trajs = m.get_traj_dicts(path)
                stitched_trajectory_queue.put(trajs[::-1])
                input_obj += len(path)
                output_obj += 1
            stitcher_logger.info("Final flushing {} raw fragments --> {} stitched fragments".format(input_obj, 
                                                                                                    output_obj),
                                 extra = None)
            break

        
    stitcher_logger.info("Exit stitcher")
  
    return   
 
 

if __name__ == '__main__':

    
    print("not implemented")
    
    
    