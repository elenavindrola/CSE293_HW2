import rpyc
from rpyc.utils.server import ThreadedServer
import collections
import time
import operator
import glob
import sys
import os
import zipfile
import urllib.request
import ujson as json
from threading import Lock, Thread
from enum import Enum

# Configuration
TASK_TIMEOUT = 20  # seconds
task_lock = Lock()
result_lock = Lock()

class TaskType(Enum):
    MAP = 1
    REDUCE = 2

class TaskStatus(Enum):
    IDLE = 1
    IN_PROGRESS = 2
    COMPLETED = 3

class CoordinatorService(rpyc.Service):
    def __init__(self, num_partitions):
        self.num_partitions = num_partitions
        self.map_tasks = []
        self.reduce_tasks = []
        self.task_assignments = {}
        self.completed_maps = 0
        self.completed_reduces = 0
        self.all_maps_done = False
        self.all_reduces_done = False
        self.worker_locations = {}
        self.reduce_results = []
        self.completed_reduce_tasks = set()
        self.ready = False  # NEW: Readiness flag
        
    def exposed_register_worker(self, worker_id, hostname):
        """Worker registers itself with coordinator."""
        with task_lock:
            self.worker_locations[worker_id] = hostname
            sys.stdout.flush()
            return True
    
    def exposed_is_ready(self):
        """Check if coordinator is ready to assign tasks."""
        return self.ready
        
    def exposed_get_task(self, worker_id):
        """Worker requests a task from coordinator."""
        with task_lock:
            current_time = time.time()
            
            # Check for timed-out tasks and reassign
            for task_id, (assigned_worker, start_time) in list(self.task_assignments.items()):
                if current_time - start_time > TASK_TIMEOUT:
                    sys.stdout.flush()
                    del self.task_assignments[task_id]
                    if task_id.startswith('map_'):
                        idx = int(task_id.split('_')[1])
                        if idx < len(self.map_tasks):
                            task_type, chunk_info, status = self.map_tasks[idx]
                            if status != TaskStatus.COMPLETED:
                                self.map_tasks[idx] = (task_type, chunk_info, TaskStatus.IDLE)
                    elif task_id.startswith('reduce_'):
                        idx = int(task_id.split('_')[1])
                        if idx < len(self.reduce_tasks):
                            task_type, partition_id, status = self.reduce_tasks[idx]
                            if status != TaskStatus.COMPLETED:
                                self.reduce_tasks[idx] = (task_type, partition_id, TaskStatus.IDLE)
            
            # Assign MAP tasks first
            if not self.all_maps_done:
                for i, (task_type, chunk_info, status) in enumerate(self.map_tasks):
                    if status == TaskStatus.IDLE:
                        task_id = f"map_{i}"
                        self.map_tasks[i] = (task_type, chunk_info, TaskStatus.IN_PROGRESS)
                        self.task_assignments[task_id] = (worker_id, current_time)
                        return (task_id, TaskType.MAP.value, chunk_info, self.num_partitions)
                
                return (None, None, None, None)
            
            # Assign REDUCE tasks after all maps are done
            if self.all_maps_done and not self.all_reduces_done:
                for i, (task_type, partition_id, status) in enumerate(self.reduce_tasks):
                    if status == TaskStatus.IDLE:
                        task_id = f"reduce_{i}"
                        self.reduce_tasks[i] = (task_type, partition_id, TaskStatus.IN_PROGRESS)
                        self.task_assignments[task_id] = (worker_id, current_time)
                        return (task_id, TaskType.REDUCE.value, partition_id, None)
                
                return (None, None, None, None)
            
            return (None, None, None, None)
    
    def exposed_submit_map_result(self, task_id, worker_id):
        """Worker notifies coordinator that map task is complete."""
        if task_id.startswith('map_'):
            idx = int(task_id.split('_')[1])
            if idx < len(self.map_tasks):
                with result_lock:
                    _, _, current_status = self.map_tasks[idx]
                    if current_status != TaskStatus.COMPLETED:
                        self.map_tasks[idx] = (TaskType.MAP, None, TaskStatus.COMPLETED)
                        self.completed_maps += 1
                        if task_id in self.task_assignments:
                            del self.task_assignments[task_id]
                        if self.completed_maps % 10 == 0 or self.completed_maps == len(self.map_tasks):
                            sys.stdout.flush()
                        return True
        return False
    
    def exposed_submit_reduce_result(self, task_id, output_file):
        """Worker submits completed reduce task - just the filename."""
        with result_lock:
            if task_id.startswith('reduce_'):
                idx = int(task_id.split('_')[1])
                
                # Check if already completed
                if task_id in self.completed_reduce_tasks:
                    sys.stdout.flush()
                    return False
                
                if idx < len(self.reduce_tasks):
                    _, _, current_status = self.reduce_tasks[idx]
                    if current_status != TaskStatus.COMPLETED:
                        self.reduce_tasks[idx] = (TaskType.REDUCE, None, TaskStatus.COMPLETED)
                        self.completed_reduce_tasks.add(task_id)
                        
                        # Read the result from the output file
                        try:
                            output_file_str = str(output_file)
                            with open(output_file_str, 'r') as f:
                                word_counts = json.load(f)
                                for word, count in word_counts.items():
                                    self.reduce_results.append((word, count))
                                result_count = len(word_counts)
                        except Exception as e:
                            sys.stdout.flush()
                            result_count = 0
                        
                        self.completed_reduces += 1
                        if task_id in self.task_assignments:
                            del self.task_assignments[task_id]
                        
                        # Check if all reduces are done
                        if self.completed_reduces == len(self.reduce_tasks):
                            self.all_reduces_done = True
                            sys.stdout.flush()
                        else:
                            sys.stdout.flush()
                        return True
        return False
    
    def exposed_is_done(self):
        """Check if all tasks are completed."""
        return self.all_maps_done and self.all_reduces_done

def download(url='https://mattmahoney.net/dc/enwik9.zip'):
    """Downloads and unzips a wikipedia dataset in txt/."""
    os.makedirs('txt', exist_ok=True)
    
    filename = url.split('/')[-1]
    filepath = filename
    
    if os.path.exists(filepath):
        print(f"{filepath} already exists, skipping download.")
    else:
        urllib.request.urlretrieve(url, filepath)
    
    with zipfile.ZipFile(filepath, 'r') as zip_ref:
        zip_ref.extractall('txt/')
    
    return glob.glob('txt/*')

def split_file_into_chunks(filename, num_chunks):
    file_size = os.path.getsize(filename)
    chunk_size = file_size // num_chunks
    
    chunks = []
    with open(filename, 'rb') as f:
        for i in range(num_chunks):
            start = i * chunk_size
            if i == num_chunks - 1:
                end = file_size
            else:
                # Seek to approximate position and find next newline
                f.seek(start + chunk_size)
                f.readline()  # Skip partial line
                end = f.tell()
            
            if start < file_size:
                chunks.append((filename, start, end))
    
    return chunks

def run_coordinator(num_workers, url):
    """Run the coordinator server and manage MapReduce."""
    # Create intermediate directory for map outputs
    os.makedirs('intermediate', exist_ok=True)
    
    num_partitions = num_workers
    coordinator = CoordinatorService(num_partitions)
    
    # Start coordinator RPC server first
    server = ThreadedServer(coordinator, port=18860, protocol_config={
        "allow_public_attrs": True,
        "sync_request_timeout": 60
    })
    
    server_thread = Thread(target=server.start, daemon=True)
    server_thread.start()
    sys.stdout.flush()
    
    # Small delay to ensure server is listening
    time.sleep(0.5)
    
    # NOW download and prepare tasks
    input_files = download(url)
    
    if not input_files:
        sys.exit(1)
    
    # Create MAP tasks
    all_chunks = []
    for filename in input_files:
        MAP_CHUNKS_PER_WORKER = int(os.environ.get('MAP_CHUNKS_PER_WORKER', 8)) 
        num_chunks = max(num_workers * MAP_CHUNKS_PER_WORKER, 16)
        chunks = split_file_into_chunks(filename, num_chunks)
        all_chunks.extend(chunks)
    
    coordinator.map_tasks = [(TaskType.MAP, chunk, TaskStatus.IDLE) for chunk in all_chunks]
    
    # Create REDUCE tasks
    coordinator.reduce_tasks = [(TaskType.REDUCE, i, TaskStatus.IDLE) for i in range(num_partitions)]
    
    # Mark coordinator as ready
    coordinator.ready = True
    sys.stdout.flush()
    
    start_time = time.time()
    
    # Wait for all map tasks to complete
    map_start = time.time()
    while coordinator.completed_maps < len(coordinator.map_tasks):
        time.sleep(0.5)
    
    # Mark maps as done
    with task_lock:
        coordinator.all_maps_done = True
    
    map_duration = time.time() - map_start
    sys.stdout.flush()
    
    # Wait for all reduce tasks to complete
    reduce_start = time.time()
    last_progress_time = time.time()
    while not coordinator.all_reduces_done:
        time.sleep(1)
        current_time = time.time()
        if int(current_time - last_progress_time) >= 5:
            sys.stdout.flush()
            last_progress_time = current_time
    
    reduce_duration = time.time() - reduce_start
    
    # Sort and display results
    sys.stdout.flush()
    from collections import Counter
    if not coordinator.reduce_results:
        print("WARNING: No results collected from reduce phase!")
    else:
        # Aggregate results
        word_totals = Counter()
        for word, count in coordinator.reduce_results:
            word_totals[word] += count
        
        word_counts = sorted(word_totals.items(), key=operator.itemgetter(1), reverse=True)
        
        print('\n TOP 20 WORDS BY FREQUENCY \n')
        top20 = word_counts[0:20]
        longest = max(len(word) for word, count in top20)
        for i, (word, count) in enumerate(top20, 1):
            print(f'{i}.\t{word:<{longest+1}}: {count:>10}')
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"\n Elapsed Time: {elapsed_time:.2f} seconds ")
    
    server.close()

if __name__ == "__main__":
    num_workers = int(os.environ.get('NUM_WORKERS', 3))
    if len(sys.argv) > 1:
        try:
            num_workers = int(sys.argv[1])
        except ValueError:
            pass
    
    url = os.environ.get('DATASET_URL', 'https://mattmahoney.net/dc/enwik9.zip')
    
    run_coordinator(num_workers, url)