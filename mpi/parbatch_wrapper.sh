#!/bin/bash

QUEUE_FILE=$1
NUM_NODES=$2
JOB_FILE=$QUEUE_FILE.joblist

module load system/parbatch

rm -f $JOB_FILE
for ((task=0; task < NUM_NODES; task++));
do
  echo "./smallworkqueue_worker.sh $QUEUE_FILE.$task" >> $JOB_FILE
done
