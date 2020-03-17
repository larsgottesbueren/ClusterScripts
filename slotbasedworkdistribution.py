#!/usr/bin/env python3

import subprocess
import argparse
import random
import itertools
import subprocess
import os
import time

my_dir = os.path.dirname(os.path.abspath(__file__))

# TODO currently we have dynamic load balancing. also offer guided, i.e. reduce number of tasks per slot when the number of remaining work drops.

MAX_TASKS_IN_SLOT_QUEUE=6

MAX_JOBS_IN_QUEUE=50
JOB_TIMELIMIT=259200			#seconds

parser = argparse.ArgumentParser()
parser.add_argument("workload", type=str)
args = parser.parse_args()
workload_file = args.workload
remaining_work_file = "{}.remaining".format(workload_file)
terminate_work_file = "{}.terminate".format(workload_file)
#workload_file = "testfile.txt"


def chunk(l, nchunks):
	chunks = [ [] for i in range(nchunks)]
	for c, w in zip( itertools.cycle(chunks), l ):
		c.append(w)
	return chunks

def serialize(workload, filename):
	tmp_filename = filename + '.tmp'
	with open(tmp_filename,'w') as f:
		for l in workload:
			f.write(l)
	os.rename(tmp_filename, filename)

def serialize_slot_work(workloads, slots):
	for w,s in zip(workloads, slots):
		serialize(w, slotqueue_filename(s))

def parse_squeue(state):
	result = []
	p,err=subprocess.Popen(["squeue", "--long", "-t " + state], stdout=subprocess.PIPE, universal_newlines=True).communicate()
	lines = p.split('\n')
	if len(lines) < 1:
		raise RuntimeError("squeue returned empty list")
	return [int(l.split()[0]) for l in lines[1:]]

def slotqueue_filename(slot):
	return "{}_slot_{}.queue".format(workload_file, slot)

def slotjobid_filename(slot):
	return "{}_slot_{}.jobid".format(workload_file, slot)

def remaining_work_of_slot(slot):
	try:
		with open(slotqueue_filename(slot),'r') as f:
			return [x for x in f]
	except FileNotFoundError:
		return []

def workload_length(slot):
	return len(remaining_work_of_slot(slot))

def has_empty_queue(slot):
	try:
		return os.path.getsize(slotqueue_filename(slot)) == 0
	except:
		return False

def is_queue_missing_or_empty(slot):
	try:
		return os.path.getsize(slotqueue_filename(slot)) == 0
	except FileNotFoundError:
		return True
	except:
		return False

def get_active_slots_with_empty_queue(active_slots):
	return list(filter(lambda slot: has_empty_queue(slot), active_slots))

def get_available_slots(active_slots):
	available_slots = set(range(MAX_JOBS_IN_QUEUE))
	for s in active_slots:
		available_slots.remove(s)
	return list(available_slots)

def retake_work(workload_list, slot):
	fn = slotqueue_filename(slot)
	if os.path.exists(fn):
		workload_list.extend(remaining_work_of_slot(slot))
		os.remove(slotqueue_filename(slot))

def submit(slot):
	if len(remaining_work) == 0:
		return

	if slot in slot2jobid:
		del jobid2slot[slot2jobid[slot]]	#slot2jobid[slot] is overridden below.
		del slot2jobid[slot]

	time_option = "-t " + str(JOB_TIMELIMIT) 
	queue_option = "--export=QUEUE_FILE=\"" + slotqueue_filename(slot) + "\"" 
	jobdesc = "sbatch -p single -n 1 --exclusive --parsable " + time_option + " " + queue_option + " ./smallworkqueue_worker.sh"
	print(jobdesc)
	out, err = subprocess.Popen([jobdesc], shell=True, stdout=subprocess.PIPE, universal_newlines=True).communicate()

	if len(out.strip()):
		jobid = int(out.strip())
		slot2jobid[slot] = jobid
		jobid2slot[jobid] = slot
		with open(slotjobid_filename(slot), 'w') as f:
			f.write(str(jobid))

def manage_jobs(try_squeue):
	active_slots = list(range(MAX_JOBS_IN_QUEUE))
	available_slots = []

	if try_squeue:
		try:
			active_jobs = parse_squeue("RUNNING, PENDING")
			active_slots = [jobid2slot[job] for job in active_jobs]
			available_slots = get_available_slots(active_slots)
		except Exception as e:
			print(e)

	old_remaining_tasks = len(remaining_work)

	for s in available_slots:
		retake_work(remaining_work, s)

	slots_with_empty_queue = get_active_slots_with_empty_queue(active_slots)
	num_active_slots_with_empty_queue = len(slots_with_empty_queue)
	num_tasks_to_distribute = min(len(slots_with_empty_queue) * MAX_TASKS_IN_SLOT_QUEUE, len(remaining_work))
	chunked_tasks = chunk(remaining_work[:num_tasks_to_distribute], len(slots_with_empty_queue))
	del remaining_work[:num_tasks_to_distribute]
	if num_tasks_to_distribute > 0 or old_remaining_tasks != len(remaining_work):	#save some IO, huh?
		serialize_slot_work(chunked_tasks, slots_with_empty_queue)
		serialize(remaining_work, remaining_work_file)

	return available_slots

def should_i_terminate():
	return os.path.exists(terminate_work_file) or (len(remaining_work) == 0 and all([is_queue_missing_or_empty(s) for s in range(MAX_JOBS_IN_QUEUE)]))

def load_remaining_work():
	if os.path.exists(remaining_work_file):
		my_wf = remaining_work_file
		print("Reading remaining work file {}".format(remaining_work_file))
	else:
		my_wf = workload_file
	with open(my_wf,'r') as wf:
		return [l for l in wf]

slot2jobid=dict()
jobid2slot=dict()

remaining_work = load_remaining_work()
#random.shuffle(remaining_work)
for s in range(MAX_JOBS_IN_QUEUE):
	# Restore jobid 2 slot mapping
	if os.path.exists(slotjobid_filename(s)):
		with open(slotjobid_filename(s), 'r') as f:
			jobid = int(f.read())
			slot2jobid[s] = jobid
			jobid2slot[jobid] = s

i = 0
while True:
	available_slots = manage_jobs(i % 100 == 0)
	if i % 100 == 0:		#3*100 seconds = 5 minutes 
		print(len(remaining_work), "unassigned tasks in remaining work.", "times woken up", i)
	i = i + 1
	if should_i_terminate():
		print("Terminating. This can be either because you created the .terminate file or because there is no more work.")
		
		try:
			os.remove(terminate_work_file)
		except:
			pass

		break
	else:
		for s in available_slots:	#do it here to eliminate potential race condition on the should_i_terminate function
			submit(s)
		time.sleep(3)	#sleep for 3 seconds
