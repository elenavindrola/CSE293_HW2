import rpyc
import string
import collections
import time
import os
import sys
import ujson as json
import glob
import re
WORD_PATTERN = re.compile(r'\b[a-z]+\b')
STOP_WORDS = set([
    'a', 'an', 'and', 'are', 'as', 'be', 'by', 'for', 'if', 'in',
    'is', 'it', 'of', 'or', 'py', 'rst', 'that', 'the', 'to', 'with',
])

TR = str.maketrans(string.punctuation, ' ' * len(string.punctuation))

def perform_map(task_id, chunk_info, num_partitions):
    """Map step: Read file chunk, tokenize, count, partition, and write to files."""
    filename, start_byte, end_byte = chunk_info
    
    # Create partitions (one per reducer)
    partitions = [collections.defaultdict(int) for _ in range(num_partitions)]
    
    try:
        with open(filename, 'rb') as f:
            f.seek(start_byte)
            bytes_read = 0
            
            while bytes_read < (end_byte - start_byte):
                line = f.readline()
                if not line:
                    break
                bytes_read += len(line)
                
                try:
                    line = line.decode('utf-8', errors='replace')
                except:
                    continue
                
                # Skip comments
                if line.lstrip().startswith('..'):
                    continue
                
                # Tokenize and count
                line = line.lower()
                for word in WORD_PATTERN.findall(line):
                    if word not in STOP_WORDS:
                        partition_id = hash(word) % num_partitions
                        partitions[partition_id][word] += 1
        
        # Write all partitions to a single file (batched)
        os.makedirs('intermediate', exist_ok=True)
        output_file = f'intermediate/{task_id}_all_partitions.json'
        
        # Convert partitions to a list of (partition_id, word_dict) tuples
        partition_data = []
        for partition_id, word_dict in enumerate(partitions):
            if word_dict:  # Only include non-empty partitions
                partition_data.append((partition_id, dict(word_dict)))
        
        # Single file write instead of multiple
        if partition_data:
            with open(output_file, 'w') as f:
                json.dump(partition_data, f)
        
        return True
    
    except Exception as e:
        sys.stdout.flush()
        return False

def perform_reduce(task_id, partition_id):
    """Reduce step: Read all map outputs for this partition, aggregate, and write to file."""
    from collections import Counter
    # Find all intermediate files (now they contain ALL partitions)
    pattern = f'intermediate/*_all_partitions.json'
    partition_files = glob.glob(pattern)
    
    sys.stdout.flush()
    
    # Read and aggregate - extract only our partition_id from each file
    aggregated = Counter()
    for filepath in partition_files:
        with open(filepath, 'r') as f:
            partition_data = json.load(f)  # This is now a list of (pid, word_dict) tuples
            for pid, word_counts in partition_data:
                if pid == partition_id:  # Only process our partition
                    aggregated.update(word_counts)
    
    unique_words = len(aggregated)
    total_count = sum(aggregated.values())
    
    # Write result to output file instead of returning it
    os.makedirs('output', exist_ok=True)
    output_file = f'output/{task_id}_result.json'
    with open(output_file, 'w') as f:
        json.dump(dict(aggregated), f)
    
    sys.stdout.flush()
    
    return output_file

def connect_to_coordinator(coordinator_host='coordinator', coordinator_port=18860, max_retries=30):
    """Connect to coordinator with adaptive retries - fast initially, then slower."""
    sys.stdout.flush()
    
    for attempt in range(max_retries):
        try:
            conn = rpyc.connect(coordinator_host, coordinator_port, config={
                "sync_request_timeout": 60,
                "allow_public_attrs": True
            })
            
            # Wait for coordinator to be ready with tasks
            ready_attempts = 0
            while ready_attempts < 20:
                try:
                    if conn.root.exposed_is_ready():
                        sys.stdout.flush()
                        return conn
                except:
                    pass
                time.sleep(0.5)
                ready_attempts += 1
            
            # If we connected but coordinator never became ready, treat as failed attempt
            conn.close()
            
        except Exception as e:
            pass
        
        if attempt < max_retries - 1:
            # Adaptive retry delay: faster early on, slower later
            if attempt < 5:
                delay = 0.5  # Fast retries for first 5 attempts
            elif attempt < 10:
                delay = 1.0  # Medium retries
            else:
                delay = 2.0  # Slower retries after that
            
            if attempt % 5 == 0:  # Only log every 5th attempt to reduce noise
                sys.stdout.flush()
            time.sleep(delay)
        else:
            sys.stdout.flush()
            raise Exception(f"Could not connect to coordinator at {coordinator_host}:{coordinator_port}")
    
    raise Exception("Connection failed")

def run_worker(worker_id, hostname):
    """Worker main loop: request tasks and execute them."""
    sys.stdout.flush()
    
    # Ensure intermediate directory exists
    os.makedirs('intermediate', exist_ok=True)
    
    # Connect to coordinator
    coordinator = connect_to_coordinator()
    
    # Register with coordinator
    coordinator.root.exposed_register_worker(worker_id, hostname)
    sys.stdout.flush()
    
    consecutive_no_tasks = 0
    
    while True:
        try:
            # Request a task from coordinator
            task_id, task_type, task_data, extra_data = coordinator.root.exposed_get_task(worker_id)
            
            if task_id is None:
                # Check if all work is done
                if coordinator.root.exposed_is_done():
                    sys.stdout.flush()
                    break
                else:
                    consecutive_no_tasks += 1
                    if consecutive_no_tasks == 1 or consecutive_no_tasks % 10 == 0:
                        sys.stdout.flush()
                    time.sleep(0.3)
                    
                    if consecutive_no_tasks > 60:
                        if coordinator.root.exposed_is_done():
                            sys.stdout.flush()
                            break
                    continue
            
            consecutive_no_tasks = 0
            
            # Execute task based on type
            if task_type == 1:  # MAP task
                num_partitions = extra_data
                sys.stdout.flush()
                
                success = perform_map(task_id, task_data, num_partitions)
                
                if success:
                    # Notify coordinator of completion
                    coordinator.root.exposed_submit_map_result(task_id, worker_id)
                    sys.stdout.flush()
                
            elif task_type == 2:  # Reduce task
                partition_id = task_data
                sys.stdout.flush()
                
                try:
                    result = perform_reduce(task_id, partition_id)
                    
                    sys.stdout.flush()
                    
                    success = coordinator.root.exposed_submit_reduce_result(task_id, result)
                    if success:
                        sys.stdout.flush()
                    
                except Exception as reduce_error:
                    import traceback
                    traceback.print_exc()
                    sys.stdout.flush()
            
        except KeyboardInterrupt:
            sys.stdout.flush()
            break
        except Exception as e:
            sys.stdout.flush()
            time.sleep(2)
            try:
                coordinator.close()
                coordinator = connect_to_coordinator()
                coordinator.root.exposed_register_worker(worker_id, hostname)
            except:
                sys.stdout.flush()
                break
    
    coordinator.close()
    sys.stdout.flush()

if __name__ == "__main__":
    worker_id = os.environ.get('HOSTNAME', 'worker-unknown')
    hostname = os.environ.get('HOSTNAME', 'worker-unknown')
    
    run_worker(worker_id, hostname)