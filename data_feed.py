#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 10 14:55:04 2022

@author: yanbing_wang
6/30
Live data read should be only one process, and distribute to 2 queues (east/west) based on document direction
two seperate live_data_feed processes will mess up the change stream
"""

import i24_logger.log_writer as log_writer
import time
import ijson
import sys

import utils.misc as misc

verbs = ["medicates", "taunts", "sweettalks", "initiates", "harasses", "negotiates", "castigates", "disputes", "cajoles", "improvises",
         "surrenders", "escalates", "mumbles", "juxtaposes", "excites", "lionizes", "ruptures", "yawns","administers","flatters","foreshadows","buckles",
         "moans", "gasps", "yells", "praises", "impersonates", "giggles", "roars", "articulates", "capitalizes", "calibrates", "protests", "conforms"]
max_trials = 10


def thread_update_one(raw, _id, filter, fitx, fity):
    filter = [1 if i else 0 for i in filter]
    raw.update_one({"_id": _id}, {"$set": {"filter": filter,
                                            "fitx": list(fitx),
                                            "fity": list(fity)}}, upsert = True)
    
def static_data_reader(default_param, db_param, raw_queue, query_filter, name=None):
    """
    Read data from a static collection, sort by last_timestamp and write to queues
    :param default_param
        :param host: Database connection host name.
        :param port: Database connection port number.
        :param username: Database authentication username.
        :param password: Database authentication password.
        :param database_name: Name of database to connect to (do not confuse with collection name).
        :param collection_name: Name of database collection from which to query.
    :param raw_queue: Process-safe queue to which records that are "ready" are written.  multiprocessing.Queue
    :param dir: "eb" or "wb"
    :param: node: (str) compute_node_id for videonode
    :return:
    """
    # Signal handling: in live data read, SIGINT and SIGUSR1 are handled in the same way    
    
    # running_mode = os.environ["my_config_section"]
    logger = log_writer.logger
    if name is None:
        name = "static_data_reader"
    logger.set_name(name)
    setattr(logger, "_default_logger_extra",  {})
     
    # get parameters for fitting
    logger.info("{} starts reading from {}.json".format(name, default_param["raw_collection"]))

    min_queue_size = default_param["min_queue_size"]
    discard = 0 # counter for short (<3) tracks
    cntr = 0

    
    with open(default_param["raw_collection"]+'.json', 'rb') as f:
        
        try:
            # keep filling the queues so that they are not low in stock
            if raw_queue.qsize() <= min_queue_size :#or west_queue.qsize() <= min_queue_size:
                
                for doc in ijson.items(f, 'item'):
                    cntr += 1

                    if len(doc["timestamp"]) > 3: 
                        # convert time series from decimal to float
                        doc["timestamp"] = list(map(float, doc["timestamp"]))
                        doc["x_position"] = list(map(float, doc["x_position"]))
                        doc["y_position"] = list(map(float, doc["y_position"]))
                        doc["width"] = list(map(float, doc["width"]))
                        doc["length"] = list(map(float, doc["length"]))
                        doc["height"] = list(map(float, doc["height"]))
                        doc["velocity"] = list(map(float, doc["velocity"]))
                        doc["detection_confidence"] = list(map(float, doc["detection_confidence"]))

                        doc["first_timestamp"] = float(doc["first_timestamp"])
                        doc["starting_x"] = float(doc["starting_x"])
                        doc["ending_x"] = float(doc["ending_x"])
                        doc["last_timestamp"] = float(doc["last_timestamp"])
                        doc["_id"] = doc["_id"]["$oid"]
                        doc["compute_node_id"] = 1
                        
                        doc = misc.interpolate(doc)
                        # print(doc["_id"]["$oid"])
                        # print(getattr(node, self.attr))
                        raw_queue.put(doc)          
                    else:
                        print("****** discard ",doc["_id"])
                        discard += 1


            # if queue has sufficient number of items, then wait before the next iteration (throttle)
            logger.info("** queue size is sufficient. wait")     
            time.sleep(2)   
         
            
        except StopIteration:  # rri reaches the end
            logger.warning("static_data_reader reaches the end of query range iteration. Exit")
        
        except Exception as e:
            logger.warning("Other exceptions occured. Exit. Exception:{}".format(str(e)))

        
    
    # logger.info("outside of while loop:qsize for raw_data_queue: east {}, west {}".format(east_queue.qsize(), west_queue.qsize()))
    logger.debug("Discarded {} short tracks".format(discard))
    logger.info("Data reader closed. Exit {}.".format(name))

    return
    

    
if __name__ == '__main__':

    import queue
    import json
    with open("parameters.json") as f:
        parameters = json.load(f)

    parameters["raw_collection"] = "iccv_raw1"
    # east_queue = queue.Queue()
    west_queue = queue.Queue()
    static_data_reader(parameters, None, west_queue, None)
    
    
    # default_param, db_param, raw_queue, query_filter, 