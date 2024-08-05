import multiprocessing as mp
import queue
import time
from collections import defaultdict
from uuid import uuid4


class StreamSeries:
    def __init__(self, queue: "queue.Queue", proc_name: list[str]):
        self._queue = queue
        self._proc_name = proc_name
    
    @property
    def queue(self):
        return self._queue
    
    @property
    def proc_name(self):
        return self._proc_name


class StreamManager:
    def __init__(self):
        self._mp_manager = mp.Manager()
        self.queues_map = {}
        self.proc_map = defaultdict(dict)
        self.pid_tracker = {}
        self.param = self._mp_manager.dict()
    
    def get_queue(self):
        return self._mp_manager.Queue()
    
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.keep_alive()
    
    def keep_alive(self):
        HB = self.param["log_heartbeat"]
        start = time.time()
        begin = start
        while True:
            now = time.time()
            if now - begin > 14400 and all([q.empty() for _,q in self.queues_map.items()]): # 4hr
                print("postproc_manager | Master processes exceed running for 4hr and all queues are empty.")
                break
            
            if (
                now - begin > 20 and
                all(q.empty() for _,q in self.queues_map.items()) and
                not any(self.proc_map[proc]["process"].is_alive() for proc in self.proc_map)
            ):
                print("postproc_manager | Master processes complete in {} sec.".format(now-begin))
                break
            
            for proc_name, proc_info in self.proc_map.items():

                if not proc_info["process"].is_alive():
                    pred_alive = (
                        [False]
                        if not proc_info["predecessor"] else
                        [self.proc_map[pred]["process"].is_alive() for pred in proc_info["predecessor"]]
                    )
                    queue_empty = (
                        [True]
                        if not proc_info["dependent_queue"] else
                        [q.empty() for q in proc_info["dependent_queue"]]
                    )
                    
                    if not any(pred_alive) and all(queue_empty): # natural death
                        # Todo: currently not used
                        proc_info["keep_alive"] = False
                    else:
                        # resurrect this process
                        print(f"postproc_manager | Resurrect {proc_name}")
                        # print(f"postproc_manager |    -   parent: {self.proc_map[proc_name]['predecessor']}")
                        # print(f"postproc_manager |    -     self: {proc_name}")
                        # print(f"postproc_manager |    -predalive: {pred_alive}")
                        # print(f"postproc_manager |    -  q empty: {queue_empty}")
                        subsys_process = proc_info['create_process']()
                        subsys_process.start()
                        self.pid_tracker[proc_name] = subsys_process.pid
                        self.proc_map[proc_name]["process"] = subsys_process 
                
            # Heartbeat queue sizes
            now = time.time()
            if now - start > HB:
                for proc_name, q in self.queues_map.items():
                    if not q.empty():
                        print("postproc_manager | Queue size for {}: {}".format(proc_name, 
                                                                            self.queues_map[proc_name].qsize()))
                print("postproc_manager | Master processes have been running for {} sec".format(now-begin))
                start = time.time()

    def pipe(self, fn, output_idx: int | None = None):
        def _fn(*args, name: str | None = None):
            res_queue = self.get_queue()
            self.queues_map[name] = res_queue
            args = list(args)
            if output_idx is not None:
                args.insert(output_idx, res_queue)
            dependent_queues: list[queue.Queue] = []
            predecessors: list[str] = []
            for i in range(len(args)):
                arg = args[i]
                if isinstance(arg, StreamSeries):
                    args[i] = arg.queue
                    dependent_queues.append(arg.queue)
                    predecessors.extend(arg.proc_name)
            if name is None:
                name = str(uuid4())
            def process():
                return mp.Process(target=fn, args=args, name=name, daemon=False)
            subsys_process = process()
            subsys_process.start()
            self.pid_tracker[name] = subsys_process.pid
            self.proc_map[name]['process'] = subsys_process
            self.proc_map[name]['dependent_queue'] = dependent_queues
            self.proc_map[name]['predecessor'] = predecessors
            self.proc_map[name]['create_process'] = process
            return StreamSeries(res_queue, [name])
        return _fn

    def merge_queues(self, *stream_series: "StreamSeries"):
        out_queue = self.get_queue()
        predecessors = sum((ss.proc_name for ss in stream_series), [])
        name_suffix = "_AND_".join(predecessors)

        merged_name = f"MERGEDQUEUE__{name_suffix}"
        self.queues_map[merged_name] = out_queue

        names: list[str] = []
        for ss in stream_series:
            name = f"MERGE_FROM__{ss.proc_name}__TO__{merged_name}"
            names.append(name)

            self.proc_map[name]['create_process'] = _create_process(name, ss, out_queue)
            self.proc_map[name]['dependent_queue'] = [ss.queue]
            self.proc_map[name]['predecessor'] = ss.proc_name

            subsys_process = self.proc_map[name]['create_process']()
            subsys_process.start()
            self.pid_tracker[name] = subsys_process.pid
            self.proc_map[name]['process'] = subsys_process
        
        return StreamSeries(out_queue, names)


def _pipe_queues(inqueue: "mp.Queue", outqueue: "mp.Queue", stream_series: "StreamSeries"):
    while True:
        try:
            outqueue.put(inqueue.get(timeout=20))
        except queue.Empty: 
            print(f"Pipe Queues (from {stream_series.proc_name}) is timed out after {20}s. Close the Pipe Queue.")
            # worker_pool.close()
            break
        except Exception as e: # other exception
            print("{}, Close the pool".format(e))
            # worker_pool.close() # wait until all processes finish their task
            break


def _create_process(name: str, ss: StreamSeries, out_queue: queue.Queue):
    def fn():
        return mp.Process(
            target=_pipe_queues,
            args=(ss.queue, out_queue, ss),
            name=name,
            daemon=False
        )
    return fn