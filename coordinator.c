/**
 * The MapReduce coordinator. The coordinator is responsible for handling job submissions, task assignments, and monitoring progress.
 * This script implements an RPC server to interact with worker nodes, receiving job submissions, polling job status, 
 * and assigning map/reduce tasks to workers.
 */

#include "coordinator.h"

#ifndef SIG_PF
#define SIG_PF void (*)(int)
#endif

/* Global coordinator state. */
coordinator* state;

extern void coordinator_1(struct svc_req*, SVCXPRT*);

/* Set up and run RPC server. */
int main(int argc, char** argv) {
  register SVCXPRT* transp;

  // Unset any previous service registration for COORDINATOR
  pmap_unset(COORDINATOR, COORDINATOR_V1);

  // Create a UDP transport for RPC communication.
  transp = svcudp_create(RPC_ANYSOCK);
  if (transp == NULL) {
    fprintf(stderr, "%s", "cannot create udp service.");
    exit(1);
  }
  if (!svc_register(transp, COORDINATOR, COORDINATOR_V1, coordinator_1, IPPROTO_UDP)) {
    fprintf(stderr, "%s", "unable to register (COORDINATOR, COORDINATOR_V1, udp).");
    exit(1);
  }

  // Create a TCP transport for RPC communication.
  transp = svctcp_create(RPC_ANYSOCK, 0, 0);
  if (transp == NULL) {
    fprintf(stderr, "%s", "cannot create tcp service.");
    exit(1);
  }
  if (!svc_register(transp, COORDINATOR, COORDINATOR_V1, coordinator_1, IPPROTO_TCP)) {
    fprintf(stderr, "%s", "unable to register (COORDINATOR, COORDINATOR_V1, tcp).");
    exit(1);
  }

  coordinator_init(&state);

  // Start the service and handle incoming requests
  svc_run();
  fprintf(stderr, "%s", "svc_run returned");
  exit(1);
  /* NOTREACHED */
}

/*
 * SUBMIT_JOB RPC implementation: Handles job submission
 * This function processes job submission requests from clients. It allocates and 
 * initializes memory for the job structure, sets unique job IDs, and stores 
 * relevant job data. It also handles memory for map and reduce tasks, tracks 
 * task completion states, validates application names, and stores the job in 
 * the job queue and job map. It ensures the output directory exists.
*/
int* submit_job_1_svc(submit_job_request* argp, struct svc_req* rqstp) {
  static int result;

  printf("Received submit job request\n");

  // allocate memory for the current job structure
  struct new_job *curr_job = malloc(sizeof(struct new_job));
  if (curr_job == NULL) {
    result = -1;
    return &result;
  }

  curr_job->job_id = state->curr_job_id;
  state->curr_job_id = state->curr_job_id + 1;

  // assign and return a unique job ID.
  result = curr_job->job_id;

  //strings need to be duplicated to avoid memory issues
  curr_job->output_dir = strdup(argp->output_dir);
  curr_job->total_reduce = argp->n_reduce;
  curr_job->total_map = argp->files.files_len;

  //initialize task states
  bool *temp_map = malloc(sizeof(bool) * curr_job->total_map);
  if (temp_map != NULL) {
    for (int i = 0; i < curr_job->total_map; i++) {
      temp_map[i] = false; // initialize all tasks as unassigned
    }
  }
  else {
    return NULL;
  }
  curr_job->map_assigned = temp_map;

  bool *temp_map_success = malloc(sizeof(bool) * curr_job->total_map);
  if (temp_map_success != NULL) {
    for (int i = 0; i < curr_job->total_map; i++) {
      temp_map_success[i] = false;
    }
  }
  else {
    return NULL;
  }
  curr_job->map_task_success = temp_map_success;
  
  
  bool *temp_reduce_success = malloc(sizeof(bool) * curr_job->total_reduce);
  if (temp_reduce_success != NULL) {
    for (int i = 0; i < curr_job->total_reduce; i++) {
      temp_reduce_success[i] = false;
    }
  }
  else {
    return NULL;
  }
  curr_job->reduce_task_success = temp_reduce_success;


  bool *temp_reduce = malloc(sizeof(bool) * curr_job->total_reduce);
  if (temp_reduce != NULL) {
    for (int i = 0; i < curr_job->total_reduce; i++) {
      temp_reduce[i] = false;
    }
  }
  else {
    return NULL;
  }
  curr_job->reduce_assigned = temp_reduce;

  time_t *temp_map_times = malloc(sizeof(time_t) * curr_job->total_map);
  if (temp_map_times != NULL) {
    for (int i = 0; i < curr_job->total_map; i++) {
      temp_map_times[i] = time(NULL);
    }
  }
  else {
    return NULL;
  }
  curr_job->map_times = temp_map_times;

  time_t *temp_reduce_times = malloc(sizeof(time_t) * curr_job->total_reduce);
  if (temp_reduce_times != NULL) {
    for (int i = 0; i < curr_job->total_reduce; i++) {
      temp_reduce_times[i] = time(NULL);
    }
  }
  else {
    return NULL;
  }
  curr_job->reduce_times = temp_reduce_times;

  // Validate the provided application name
  if (get_app(argp->app).name != NULL) {
    curr_job->app = strdup(argp->app);
  }
  else {
    result = -1;
    return &result;
  }

  //files.files_len from client.c
  char **curr_input_files = malloc(sizeof(char*) * argp->files.files_len);
  if (curr_input_files == NULL) {
    result = -1;
    return &result;
  }
  for (int i = 0; i < argp->files.files_len; i++) {
    curr_input_files[i] = strdup(argp->files.files_val[i]);
  }
  curr_job->input_files = curr_input_files;

  curr_job->args = NULL;
  if (argp->args.args_val != NULL) {
    curr_job->args = strdup(argp->args.args_val);
  }

  //map_finished should equal number of input files for it to be finishes
  curr_job->map_jobs_finished = 0;

  //reduce_finished should equal n_reduce for it to be finished
  curr_job->reduce_jobs_finished = 0;
  curr_job->success = false;
  curr_job->failed = false;
  curr_job->done = false;

  //we dont want the actual struct pieces to be cast
  int job_id_to_pointer = curr_job->job_id;
  // Keep track of the current order of jobs in the queue
  state->job_queue = g_list_append(state->job_queue, GINT_TO_POINTER(job_id_to_pointer));

  int another_job_id_to_pointer = curr_job->job_id;
  // Store job information in a manner that allows quick lookup by job ID
  // key is of type: gconstpointer
  g_hash_table_insert(state->job_map, GINT_TO_POINTER(another_job_id_to_pointer), curr_job);

  struct stat st;
  if (stat(argp->output_dir, &st) == -1) {
    mkdirp(argp->output_dir);
  }

  return &result;
}

/* POLL_JOB RPC implementation: checks the status of a submitted job */
poll_job_reply* poll_job_1_svc(int* argp, struct svc_req* rqstp) {
  static poll_job_reply result;

  printf("Received poll job request\n");

  
  static struct new_job *curr_job;

  //if there are no jobs in the job map, return an invalid job ID response
  if (g_hash_table_size(state->job_map) == 0) {
    result.invalid_job_id = true;
    return &result;
  }

  //look up the job in the hash table using the provided job ID
  curr_job = g_hash_table_lookup(state->job_map, GINT_TO_POINTER(*argp));

  //if the job is not found or has an invalid job ID, return an invalid job ID response
  if (curr_job == NULL || curr_job->job_id < 0) {
    result.invalid_job_id = true;
    return &result;
  }
  result.done = curr_job->done;
  result.failed = curr_job->failed;
  result.invalid_job_id = false;

  return &result;
}

/* GET_TASK RPC implementation. */
get_task_reply* get_task_1_svc(void* argp, struct svc_req* rqstp) {
  static get_task_reply result;

  printf("Received get task request\n");
  result.file = "";
  result.output_dir = "";
  result.app = "";
  result.wait = true;
  result.args.args_len = 0;

  GList *curr_elem;
  static struct new_job *curr_job;

  if (g_list_length(state->job_queue) == 0 || g_hash_table_size(state->job_map) == 0) {
    return &result;
  }

  //getting next available task from the waiting queue
  for (curr_elem = state->job_queue; curr_elem; curr_elem=curr_elem->next) {
    int curr_job_id = GPOINTER_TO_INT(curr_elem->data);
    //lookup
    curr_job = g_hash_table_lookup(state->job_map, GINT_TO_POINTER(curr_job_id));
    //int to pointer(needed in the list) to return job


    //are all map tasks done
    if (curr_job->map_jobs_finished == curr_job->total_map) {

      //all map tasks are done --> assigning reduce tasks
      for (int i = 0; i < curr_job->total_reduce; i++) {


        //need to also check how long its been since its been assigned for worker crashes
        //|| time(NULL) - reduce_assigned_time[i] >= TIMEOUT
        //FOR WORKER CRASHES
        if (curr_job->reduce_assigned[i] &&
          !curr_job->reduce_task_success[i] &&
          time(NULL) - curr_job->reduce_times[i] > TASK_TIMEOUT_SECS) {

          result.reduce = true;
          curr_job->reduce_assigned[i] = true;
          curr_job->reduce_times[i] = time(NULL);

          result.task = i;
          result.wait = false;
          result.job_id = curr_job->job_id;
          result.output_dir = strdup(curr_job->output_dir);
          result.app = strdup(curr_job->app);
          result.n_reduce = curr_job->total_reduce;
          result.n_map = curr_job->total_map;

          if (curr_job->args != NULL) {
            result.args.args_val = strdup(curr_job->args);
            result.args.args_len = strlen(curr_job->args);
          }
          return &result;
        }
        
        else if (!curr_job->reduce_assigned[i]) {
          //assign reduce task
          result.reduce = true;
          curr_job->reduce_assigned[i] = true;
          curr_job->reduce_times[i] = time(NULL);

          result.task = i;
          result.wait = false;
          result.job_id = curr_job->job_id;
          result.output_dir = strdup(curr_job->output_dir);
          result.app = strdup(curr_job->app);
          result.n_reduce = curr_job->total_reduce;
          result.n_map = curr_job->total_map;

          if (curr_job->args != NULL) {
            result.args.args_val = strdup(curr_job->args);
            result.args.args_len = strlen(curr_job->args);
          }
          return &result;
        }
      }
    }
    else {
      //all map tasks are not done --> assigning map tasks
      for (int i = 0; i < curr_job->total_map; i++) {

        //need to also check how long its been since its been assigned
        // || time(NULL) - map_assigned_time[i] >= TIMEOUT
        //FOR WORKER CRASHES
        if (curr_job->map_assigned[i] &&
          !curr_job->map_task_success[i] &&
          time(NULL) - curr_job->map_times[i] > TASK_TIMEOUT_SECS) {

          result.reduce = false;
          curr_job->map_assigned[i] = true;
          curr_job->map_times[i] = time(NULL);
          result.task = i;
          result.wait = false;
          result.file = strdup(curr_job->input_files[i]);
          result.job_id = curr_job->job_id;
          result.output_dir = strdup(curr_job->output_dir);
          result.app = strdup(curr_job->app);
          result.n_reduce = curr_job->total_reduce;
          result.n_map = curr_job->total_map;

          if (curr_job->args != NULL) {
            result.args.args_val = strdup(curr_job->args);
            result.args.args_len = strlen(curr_job->args);
          }
          return &result;
          
        }

        else if (!curr_job->map_assigned[i]) {
          //assign map task
          result.reduce = false;
          curr_job->map_assigned[i] = true;
          curr_job->map_times[i] = time(NULL);
          result.task = i;
          result.wait = false;
          result.file = strdup(curr_job->input_files[i]);
          result.job_id = curr_job->job_id;
          result.output_dir = strdup(curr_job->output_dir);
          result.app = strdup(curr_job->app);
          result.n_reduce = curr_job->total_reduce;
          result.n_map = curr_job->total_map;

          if (curr_job->args != NULL) {
            result.args.args_val = strdup(curr_job->args);
            result.args.args_len = strlen(curr_job->args);
          }

          return &result;
        }
      }
    }
  }


  return &result;
}

/* FINISH_TASK RPC implementation. */
void* finish_task_1_svc(finish_task_request* argp, struct svc_req* rqstp) {
  static char* result;

  printf("Received finish task request\n");

  static struct new_job* curr_job;

  static int curr_job_id;
  curr_job_id = argp->job_id;

  if (g_list_length(state->job_queue) == 0 || g_hash_table_size(state->job_map) == 0) {
    return (void*)&result;
  }

  curr_job = g_hash_table_lookup(state->job_map, GINT_TO_POINTER(argp->job_id));

  if(argp->success) {
    if (!argp->reduce) {
      curr_job->map_jobs_finished++;
      curr_job->map_task_success[argp->task] = true;
    }
    else {
      curr_job->reduce_jobs_finished++;
      curr_job->reduce_task_success[argp->task] = true;
      if (curr_job->reduce_jobs_finished == curr_job->total_reduce) {
        curr_job->done = true;
        curr_job->success = true;
        curr_job->failed = false;
      }
    }
  } 
  else {
    curr_job->failed = true;
    curr_job->done = true;
  }

  if (curr_job->done) {
    state->job_queue = g_list_remove(state->job_queue, GINT_TO_POINTER(argp->job_id));

  }

  return (void*)&result;
}

/* Initialize coordinator state. */
void coordinator_init(coordinator** coord_ptr) {
  *coord_ptr = malloc(sizeof(coordinator));

  coordinator* coord = *coord_ptr;

  coord->curr_job_id = 0;
  coord->job_queue = NULL;
  coord->job_map = g_hash_table_new_full(g_direct_hash, g_direct_equal, NULL, NULL);
}
