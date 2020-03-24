#!/bin/bash

# Only for testing
# QUEUE_FILE="$HOME/ClusterScripts/mpi/2020-03-24_patoh_experiments/tmp_workload.txt"

TASK_CRUNSHER="$HOME/ClusterScripts/mpi/task_crunsher"

module load compiler/gnu/9.2
module load mpi/openmpi/4.0
module load devel/python/3.7.4_gnu_9.2

mpirun --bind-to core --map-by core -report-bindings $TASK_CRUNSHER $QUEUE_FILE