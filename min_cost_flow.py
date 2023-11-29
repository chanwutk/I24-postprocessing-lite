#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May 15 15:17:59 2022

@author: yanbing_wang
"""
import queue
import time

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
 
    if not name:
        name = "stitcher_"+direction
    print(f"{name} | min_cost_flow_online_alt_path starts")
    
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
                print("Getting from fragment_queue timed out after {} sec.".format(GET_TIMEOUT))
                all_paths = m.get_all_traj()
                
                for path in all_paths:
                    trajs = m.get_traj_dicts(path)
                    stitched_trajectory_queue.put(trajs[::-1])
                    input_obj += len(path)
                    output_obj += 1
            
                print("Final flushing {} raw fragments --> {} stitched fragments".format(input_obj, output_obj))
                break

            fgmt_id = fgmt[ATTR_NAME]
            
            # ============ Add node ============
            m.add_node(fgmt)
            
            # ============ Path augment ============
            m.augment_path(fgmt_id)
            
            # ============ Pop path ============
            all_paths = m.pop_path(time_thresh = fgmt["first_timestamp"] - TIME_WIN)  
            
            num_cache = len(m.cache)
            num_nodes = m.G.number_of_nodes()
            
            for path in all_paths:
                # print("pop path", path)
                trajs = m.get_traj_dicts(path)
                stitched_trajectory_queue.put(trajs[::-1])
                m.clean_graph(path)
                
                input_obj += len(path)
                output_obj += 1
                
            # heartbeat log
            now = time.time()
            if now - begin > HB:
                print("MCF graph # nodes: {}, # edges: {}, deque: {}, cache: {}".format(m.G.number_of_nodes(), m.G.number_of_edges(), len(m.in_graph_deque), len(m.cache)))
                print("{} raw fragments --> {} stitched fragments".format(input_obj, output_obj))
                begin = time.time()
            
        except (ConnectionResetError, BrokenPipeError, EOFError) as e:   
            print("Connection error: {}".format(str(e)))
            break
            
        except Exception as e: # other unknown exceptions are handled as error TODO UNTESTED CODE!
            print("Other error: {}, push all processed trajs to queue".format(e))
            
            all_paths = m.get_all_traj()
            for path in all_paths:
                # print("exception path", path)
                trajs = m.get_traj_dicts(path)
                stitched_trajectory_queue.put(trajs[::-1])
                input_obj += len(path)
                output_obj += 1
            print("Final flushing {} raw fragments --> {} stitched fragments".format(input_obj, output_obj))
            break

        
    print("Exit stitcher")
  
    return   
 
 

if __name__ == '__main__':

    
    print("not implemented")
    
    
    
