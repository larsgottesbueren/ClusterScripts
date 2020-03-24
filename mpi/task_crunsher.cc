#include <mpi.h>
#include <vector>
#include <algorithm>
#include <thread>
#include <chrono>
#include <stdio.h>
#include <iostream>
#include <fstream>

// Number of steps an MPI task waits for new jobs
static constexpr size_t MAX_IDLE_STEPS = 100;
// Time in milliseconds a task sleep if no jobs are available
static constexpr size_t SLEEP_TIME = 3000;
static constexpr std::chrono::milliseconds CHRONO_SLEEP_TIME = std::chrono::milliseconds(SLEEP_TIME);

bool file_exists(const std::string& filename) {
  std::ifstream test(filename);
  return test.good();
}

bool is_file_empty(const std::string& filename) {
  std::ifstream test(filename);
  return test.peek() == std::ifstream::traits_type::eof();
}

void create_file(const std::string& filename) {
  std::ofstream create_file_stream(filename);
  create_file_stream << "";
  create_file_stream.close();
}

std::vector<std::string> read_jobs(const std::string& filename) {
  std::ifstream job_file(filename);
  std::string job;
  std::vector<std::string> jobs;
  while ( std::getline(job_file, job) ) {
    jobs.push_back(job);
  }
  job_file.close();
  return jobs;
}

void write_jobs(const std::string& filename, const std::vector<std::string>& jobs) {
  std::ofstream job_stream(filename);
  for ( const std::string& job : jobs ) {
    job_stream << job << std::endl;
  }
  job_stream.close();
}

void distribute_jobs(const std::string& queue_file, const int num_tasks) {
  std::vector<std::string> jobs = read_jobs(queue_file);
  std::random_shuffle(jobs.begin(), jobs.end());

  size_t num_jobs = jobs.size();
  size_t steps = std::max(1UL, num_jobs / num_tasks + ( num_jobs % num_tasks != 0 ));
  for ( int task = 0; task < num_tasks; ++task ) {
    const size_t start = task * steps;
    const size_t end = std::min(num_jobs, (task + 1) * steps);
    if ( start >= end || start >= num_jobs ) {
      break;
    }

    std::string local_queue_file = queue_file + "." + std::to_string(task);
    std::ofstream local_stream(local_queue_file);
    for ( size_t pos = start; pos < end; ++pos ) {
      local_stream << jobs[pos] << std::endl;
    }
    local_stream.close();
  }
}

int main(int argc, char** argv) {
    if ( argc != 2 ) {
      std::exit(-1);
    }

    // Initialize the MPI environment
    MPI_Init(&argc, &argv);

    // Get the rank of the process
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    std::string queue_file(argv[1]);
    std::string local_queue_file = queue_file + "." + std::to_string(rank);

    // Check if queue file exists
    if ( !file_exists(queue_file) ) {
      create_file(queue_file);
    }

    size_t steps = 0;
    std::vector<std::string> failed_jobs;
    bool waiting_for_new_jobs = true;
    while ( steps < MAX_IDLE_STEPS ) {
      if ( !file_exists(local_queue_file) || is_file_empty(local_queue_file) ) {
        MPI_Barrier(MPI_COMM_WORLD);
        // At this point all MPI tasks are idle

        if ( rank == 0 ) {
          // No tasks are running any more try to distribute work
          if ( file_exists(queue_file) && !is_file_empty(queue_file) ) {
            if ( waiting_for_new_jobs ) {
              // Global queue file exists => distribute work to local queues
              distribute_jobs(queue_file, size);
              waiting_for_new_jobs = false;
            } else {
              // Current queue is finished clear file and wait for new jobs
              create_file(queue_file);
              waiting_for_new_jobs = true;
            }
          } else {
            // No work in global queue => wait for new jobs
            ++steps;
            std::cout << "No jobs found in file '" << queue_file << "'. "
                      << "Task terminates in " << (MAX_IDLE_STEPS - steps) * SLEEP_TIME
                      << "ms." << std::endl;
          }
        }

        std::this_thread::sleep_for(CHRONO_SLEEP_TIME);
        MPI_Bcast(&steps, 1, MPI_UNSIGNED_LONG, 0, MPI_COMM_WORLD);
      } else {
        steps = 0;
        std::vector<std::string> jobs = read_jobs(local_queue_file);

        while ( !jobs.empty() ) {
          std::string job = jobs.back();
          jobs.pop_back();

          // Execute jobs
          std::cout << "Execute = '" << job << "'" << std::endl;
          int ret = std::system(job.c_str());

          if ( ret != 0 ) {
            failed_jobs.push_back(job);
            write_jobs(local_queue_file + ".failed", failed_jobs);
          }
          write_jobs(local_queue_file, jobs);
        }
      }
    }

    // Finalize the MPI environment.
    MPI_Finalize();
}