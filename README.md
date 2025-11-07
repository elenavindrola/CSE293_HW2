Dockerizing a MapReduce-style Word Count

A. Deployment

1. The following tools are needed to replicate this project:
- GitHub to clone the repository
- Docker and Docker Compose to dockerize the mapreduce with multiple images and containers
Please make sure the Docker daemon is running before running the bash scripts

2. Clone the Repository
____________________________________________________________
bash<br>
git clone https://github.com/elenavindrola/CSE293_HW2.git<br>
cd CSE293_HW2
____________________________________________________________
4. Build Docker Images
Make the build script executable and build both the coordinator and worker images:
____________________
bash<br>
chmod +x ./build.sh<br>
./build.sh
____________________
This creates two Docker images:
mapreduce-coordinator:latest
mapreduce-worker:latest

4. Spin up the containers and run Map Reduce:
_____________________________
bash<br>
chmod +x ./run_experiment.sh<br>
./run_experiment.sh <n>
_____________________________
Replace <n> with the number of workers (1, 2, 4, or 8 only). 
Please note that in step 3 and 4 you only need the chmod +x command the first time you try to run the project after downloading it.

5. Cleanup
- After execution, the script automatically removes these two directories:
intermediate/: map result files
output/: reduce result files
- After completing the mapreduce, it is recommended to run the following command to remove the images created by this project:
__________________________________________________
bash<br>
docker rmi mapreduce-worker mapreduce-coordinator
___________________________________________________
- It is possible to also clean the build history through the Docker Desktop interface.

B. Requirements:

1. The solution implements the following components:
- Coordinator Container: a single coordinator that manages task distribution, tracks worker progress, handles worker failures, and aggregates final results. 
- Worker Containers: multiple workers (configurable: 1, 2, 4, or 8) that execute Map and Reduce tasks in parallel. 
- The coordinator communicates with workers via RPyC on port 18860.

2. This project downloads and reads enwik9 as default file. You can update the URL with a different file to be downloaded at the following locations:
- Dockerfile.coordinator (line 20)
- docker-compose_n.yml files (line 7)
- coordinator.py (lines 165 and 306)
In doing so, it is recommended to delete the old file from the txt/ directory, or delete entire txt/ folder.

3. Workers  communicate with the coordinator through the following RPyC calls defined in the coordinator.py and utilized in the worker.py scripts:
- exposed_is_ready(): workers wait for coordinator to be ready with tasks
- exposed_register_worker(): workers register themselves with the coordinator
- exposed_get_task(): workers request Map or Reduce tasks
- exposed_submit_map_result(): workers notify coordinator of completion of Map tasks
- exposed_submit_reduce_result(): workers submit Reduce task outputs
- exposed_is_done(): workers check if all tasks are completed

4. When a workers performs a map task, it runs the following processes:
- it reads the input chunk assigned by the coordinator 
- it tokenizes the text converting it to lowercase, extracting only alphabetic words, and filtering out stop words like "the", "a", "and"
- it counts words occurencies
- it creates n partitions, where n = number of workers; the partitions are dictionaries
- it assigns words to partitions using 
partition_id = hash(word) % num_partitions
where num_partitions = n = number of workers
- it converts partitions to a list of tuples formatted as 
[(partition_id, {"word1":occurences1, word2:occurences2, ...}) , ...]
- it writes the tuples to JSON files in the "intermediate" directory
- it notifies the coordinator when a Map task is completed.

5. Once all map tasks complete, the coordinator assigns reduce tasks. Each worker performs the following processes:
- it reads all intermediate files from the "intermediate" directory
- it extracts word counts for its assigned partition ID
- it aggregates counts across all map outputs for that partition
- it writes the final word counts to the "output" directory

6. After all Reduce tasks complete, the coordinator performs the following processes:
- it reads all output files
- it aggregates total word counts across all partitions
- it sorts words by frequency
- it displays the top 20 most frequent words
Since the workers write to their own separate intermediate and output files, and the coordinator access output after all workers are done, there are no major concerns with concurrent access to files.
However, the following locks are established to prevent race conditions:
- task assignment lock (task_lock): protects the task queue and assignment tracking 
- result lock (result_lock): ensures mutual exclusion when workers submit completed map and reduce tasks

7. The coordinator implements a timeout mechanism of 20 seconds to detect unresponsive workers. If a worker exceeds this timeout:
- the task assignment is removed
- the task status is reset to IDLE
- the task becomes available for reassignment to another worker

8. Once map and reduce are finished, server.close() ensures the coordinator exits. Moroever, the run_experiment.sh n script ensures the containers stop running after execution with the command:
docker compose -f docker-compose_${NUM}.yml down

9. The system is configurable to run with 1, 2, 4, or 8 workers. If fact, there are four different docker-compose_n.yml files available in this repository, each of which has been customized to run with n workers:
- docker-compose_1.yml
- docker-compose_2.yml
- docker-compose_4.yml
- docker-compose_8.yml
The following bash script has been created to automate selection: 
./run_experiment.sh <n>
where <n> is the number of workers.
