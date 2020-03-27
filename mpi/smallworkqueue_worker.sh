#!/bin/bash

QUEUE_FILE=$1
TMP_QUEUE_FILE=$QUEUE_FILE".tmp"
FAILED_QUEUE_FILE=$QUEUE_FILE".failed"
DEBUG_FILE=$QUEUE_FILE".debug"

module unload mpi/openmpi/4.0
module unload devel/python/3.8.1_intel_19.1-pipenv
module unload compiler/intel/19.1
module load compiler/gnu/9.2
module load devel/python/3.7.4_gnu_9.2
module load mpi/openmpi/4.0

MAX_IDLE_STEPS=100
STEP=0
SLEEP_TIME=3	#in seconds

if [ ! -f $QUEUE_FILE ]
then
	echo "Queue file" $QUEUE_FILE "not found, creating"
	touch $QUEUE_FILE
fi

while [ $STEP -lt $MAX_IDLE_STEPS ]
do
	if [ ! -s $QUEUE_FILE ]
	then
		STEP=$((STEP+1))
		#echo "Queue empty. Sleep for $SLEEP_TIME" >> $DEBUG_FILE
		sleep $SLEEP_TIME"s"
	else
		STEP=0
		LINE=$(head -n 1 $QUEUE_FILE)
		echo "Working on $LINE"
		if eval $LINE
		then
			echo "Finished $LINE"
		else
			echo "Failed $LINE"
			echo "$LINE" >> $FAILED_QUEUE_FILE
		fi
		tail -n +2 $QUEUE_FILE > $TMP_QUEUE_FILE && mv $TMP_QUEUE_FILE $QUEUE_FILE
	fi
done
