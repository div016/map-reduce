/**
 * Logic for job and task management.
 *
 */

#ifndef JOB_H__
#define JOB_H__

#include <stdbool.h>
#include <time.h>
#include <stdio.h>
#include <unistd.h>


struct new_job {
    int job_id;
    char** input_files;
    char* output_dir;
    char* app;
    int total_reduce;
    int total_map;
    char* args;

    bool success;
    bool failed;

    int map_jobs_finished;
    time_t* map_times;
    bool* map_assigned;
    bool* map_task_success;

    int reduce_jobs_finished;
    time_t* reduce_times;
    bool* reduce_assigned;
    bool* reduce_task_success;

    bool done;
};

#endif
