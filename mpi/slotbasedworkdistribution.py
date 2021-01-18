#!/usr/bin/env python3

import subprocess
import argparse
import random
import itertools
import subprocess
import os
import time
import glob

my_dir = os.path.dirname(os.path.abspath(__file__))

MAX_TASKS_IN_SLOT_QUEUE=6
MAX_JOBS_IN_QUEUE=25
NODES_PER_JOB=2
TASKS_PER_NODE=1
TASKS_PER_JOB = NODES_PER_JOB * TASKS_PER_NODE

parser = argparse.ArgumentParser()
parser.add_argument("workload", type=str)
args = parser.parse_args()
workload_file = args.workload
remaining_work_file = "{}.remaining".format(workload_file)
terminate_work_file = "{}.terminate".format(workload_file)


def chunk(l, nchunks):
  chunks = [ [] for i in range(nchunks)]
  for c, w in zip( itertools.cycle(chunks), l ):
    c.append(w)
  return chunks

def serialize(workload, filename):
  tmp_filename = filename + '.distributor.tmp'
  with open(tmp_filename,'w') as f:
    for l in workload:
      f.write(l)
  os.rename(tmp_filename, filename)

def serialize_work(workloads, queue_files):
  for w,q in zip(workloads, queue_files):
    serialize(w, q)

def parse_squeue(state):
  parse_desc = "squeue --long -t " + state
  p, err = subprocess.Popen([parse_desc], shell=True, stdout=subprocess.PIPE, universal_newlines=True).communicate()
  lines = p.split('\n')
  if len(lines) < 3:
    raise RuntimeError("squeue returned nothing")
  return [int(l.split()[0]) for l in lines[2:-1]]

def slotqueue_filename(slot):
  return "{}_slot_{}.queue".format(workload_file, slot)

def slotqueue_task_filename(slot, task):
  return "{}_slot_{}.queue.{}".format(workload_file, slot, task)

def slotjobid_filename(slot):
  return "{}_slot_{}.jobid".format(workload_file, slot)

def remaining_work_of_slot_task(slot, task):
  try:
    with open(slotqueue_task_filename(slot, task),'r') as f:
      return [x for x in f]
  except FileNotFoundError:
    return []

def is_queue_missing_or_empty(slot):
  for task in range(TASKS_PER_JOB):
    try:
      if os.path.getsize(slotqueue_task_filename(slot, task)) != 0:
        return False
    except FileNotFoundError:
      pass
  return True

def get_empty_queue_files_of_active_slots(active_slots):
  empty_queue_files = []
  for slot in active_slots:
    for task in range(TASKS_PER_JOB):
      queue_file = slotqueue_task_filename(slot, task)
      try:
        if os.path.getsize(queue_file) == 0:
          empty_queue_files.append(queue_file)
      except:
        pass
  return empty_queue_files

def get_available_slots(active_slots):
  available_slots = set(range(MAX_JOBS_IN_QUEUE))
  for s in active_slots:
    available_slots.remove(s)
  return list(available_slots)

def retake_work(workload_list, slot):
  for task in range(TASKS_PER_JOB):
    fn = slotqueue_task_filename(slot, task)
    if os.path.exists(fn):
      workload_list.extend(remaining_work_of_slot_task(slot, task))
      os.remove(fn)

def submit(slot):
  if len(remaining_work) == 0:
    return

  if slot in slot2jobid:
    del jobid2slot[slot2jobid[slot]]
    del slot2jobid[slot]
  time_option = "-t 48:00:00"
  parbatch_args = slotqueue_filename(slot) + " " + str(TASKS_PER_JOB)
  node_options = "-N " + str(NODES_PER_JOB) + " -n " + str(NODES_PER_JOB) + " --ntasks-per-node=" + str(TASKS_PER_NODE)
  jobdesc = "sbatch -p multiple " + node_options + " --exclusive --parsable " + time_option + " ./parbatch_wrapper.sh " + parbatch_args
  print("submit job with the command: ", jobdesc)
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
      active_jobs = parse_squeue("RUNNING,PENDING")
      active_slots = [jobid2slot[job] for job in active_jobs]
      print("active slots=", active_slots)
      available_slots = get_available_slots(active_slots)
      if active_slots == []:
        print("parse_squeue. active slots empty. submit all. available_slots = ", available_slots)
      else:
        print("parse_squeue. newly available_slots = ", available_slots)
    except Exception as e:
      print(e)

  old_remaining_tasks = len(remaining_work)

  for s in available_slots:
    retake_work(remaining_work, s)

  empty_queue_files = get_empty_queue_files_of_active_slots(active_slots)
  num_tasks_to_distribute = min(len(empty_queue_files) * MAX_TASKS_IN_SLOT_QUEUE, len(remaining_work))
  chunked_tasks = chunk(remaining_work[:num_tasks_to_distribute], len(empty_queue_files))
  del remaining_work[:num_tasks_to_distribute]
  if num_tasks_to_distribute > 0 or old_remaining_tasks != len(remaining_work):	#save some IO, huh?
    serialize_work(chunked_tasks, empty_queue_files)
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
    print("Reading initial workload file ", workload_file)
  with open(my_wf,'r') as wf:
    return [l for l in wf]

slot2jobid=dict()
jobid2slot=dict()

remaining_work = load_remaining_work()
for s in range(MAX_JOBS_IN_QUEUE):
  # Restore jobid 2 slot mapping
  if os.path.exists(slotjobid_filename(s)):
    with open(slotjobid_filename(s), 'r') as f:
      jobid = int(f.read())
      slot2jobid[s] = jobid
      jobid2slot[jobid] = s

i = 0
while True:
  # Remove core dump files
  for coredump_file in glob.glob("core*"):
    os.remove(coredump_file)

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
    for s in available_slots:
      submit(s)
    time.sleep(3)	#sleep for 3 seconds
