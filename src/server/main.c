#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include "constants.h"
#include "io.h"
#include "../common/protocol.h"
#include "operations.h"
#include "parser.h"
#include "pthread.h"
#include <sys/stat.h>
#include "kvs.h"
#include <semaphore.h>
#include <signal.h>




struct SharedData {
  DIR *dir;
  char *dir_name;
  pthread_mutex_t directory_mutex;
};

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;


sigset_t set;
volatile sig_atomic_t sigusr1_received = 0;

int clients[MANAGING_THREADS];

size_t active_backups = 0; // Number of active backups
size_t max_backups;        // Maximum allowed simultaneous backups
size_t max_threads;        // Maximum allowed simultaneous threads
char *registration_pipe_name = NULL;  
char *jobs_directory = NULL;

sem_t empty_slots; 
sem_t filled_slots;

Queue client_queue;


void handle_sigusr1(int sig) {
  if (sig == SIGUSR1){
    sigusr1_received = 1;
  }
}


int filter_job_files(const struct dirent *entry) {
  const char *dot = strrchr(entry->d_name, '.');
  if (dot != NULL && strcmp(dot, ".job") == 0) {
    return 1; // Keep this file (it has the .job extension)
  }
  return 0;
}

static int entry_files(const char *dir, struct dirent *entry, char *in_path,
                       char *out_path) {
  const char *dot = strrchr(entry->d_name, '.');
  if (dot == NULL || dot == entry->d_name || strlen(dot) != 4 ||
      strcmp(dot, ".job")) {
    return 1;
  }

  if (strlen(entry->d_name) + strlen(dir) + 2 > MAX_JOB_FILE_NAME_SIZE) {
    fprintf(stderr, "%s/%s\n", dir, entry->d_name);
    return 1;
  }

  strcpy(in_path, dir);
  strcat(in_path, "/");
  strcat(in_path, entry->d_name);

  strcpy(out_path, in_path);
  strcpy(strrchr(out_path, '.'), ".out");

  return 0;
}

static int run_job(int in_fd, int out_fd, char *filename) {

  sigemptyset(&set);
  sigaddset(&set, SIGUSR1);
  if (pthread_sigmask(SIG_BLOCK, &set, NULL) != 0){
    perror("sigmask\n");
  }

  size_t file_backups = 0;
  while (1) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    switch (get_next(in_fd)) {
    case CMD_WRITE:

      num_pairs =
          parse_write(in_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);

      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_write(num_pairs, keys, values)) {
        write_str(STDERR_FILENO, "Failed to write pair\n");
      }
      break;

    case CMD_READ:
      num_pairs =
          parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_read(num_pairs, keys, out_fd)) {
        write_str(STDERR_FILENO, "Failed to read pair\n");
      }
      break;

    case CMD_DELETE:
      num_pairs =
          parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_delete(num_pairs, keys, out_fd)) {
        write_str(STDERR_FILENO, "Failed to delete pair\n");
      }
      break;

    case CMD_SHOW:
      kvs_show(out_fd);
      break;

    case CMD_WAIT:
      if (parse_wait(in_fd, &delay, NULL) == -1) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (delay > 0) {
        printf("Waiting %d seconds\n", delay / 1000);
        kvs_wait(delay);
      }
      break;

    case CMD_BACKUP:
      pthread_mutex_lock(&n_current_backups_lock);
      if (active_backups >= max_backups) {
        wait(NULL);
      } else {
        active_backups++;
      }
      pthread_mutex_unlock(&n_current_backups_lock);
      int aux = kvs_backup(++file_backups, filename, jobs_directory);

      if (aux < 0) {
        write_str(STDERR_FILENO, "Failed to do backup\n");
      } else if (aux == 1) {
        return 1;
      }
      break;

    case CMD_INVALID:
      write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
      break;

    case CMD_HELP:
      write_str(STDOUT_FILENO,
                "Available commands:\n"
                "  WRITE [(key,value)(key2,value2),...]\n"
                "  READ [key,key2,...]\n"
                "  DELETE [key,key2,...]\n"
                "  SHOW\n"
                "  WAIT <delay_ms>\n"
                "  BACKUP\n" // Not implemented
                "  HELP\n");

      break;

    case CMD_EMPTY:
      break;

    case EOC:
      printf("EOF\n");
      return 0;
    }
  }
}

// frees arguments
static void *get_file(void *arguments) {
  struct SharedData *thread_data = (struct SharedData *)arguments;
  DIR *dir = thread_data->dir;
  char *dir_name = thread_data->dir_name;

  if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to lock directory_mutex\n");
    return NULL;
  }

  struct dirent *entry;
  char in_path[MAX_JOB_FILE_NAME_SIZE], out_path[MAX_JOB_FILE_NAME_SIZE];
  while ((entry = readdir(dir)) != NULL) {
    if (entry_files(dir_name, entry, in_path, out_path)) {
      continue;
    }

    if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to unlock directory_mutex\n");
      return NULL;
    }

    int in_fd = open(in_path, O_RDONLY);
    if (in_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open input file: ");
      write_str(STDERR_FILENO, in_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out_fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (out_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open output file: ");
      write_str(STDERR_FILENO, out_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out = run_job(in_fd, out_fd, entry->d_name);

    close(in_fd);
    close(out_fd);

    if (out) {
      if (closedir(dir) == -1) {
        fprintf(stderr, "Failed to close directory\n");
        return 0;
      }

      exit(0);
    }

    if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to lock directory_mutex\n");
      return NULL;
    }
  }

  if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to unlock directory_mutex\n");
    return NULL;
  }

  pthread_exit(NULL);
}








void process_client_commands(int client_fd) {
  struct {
    char opcode;
    union {
      struct {
        char req_pipe[40];
        char resp_pipe[40];
        char notif_pipe[40];
      } connect;
      char key[40];
    } data;
  } message;
  int resp_fd;

  while (1) {
    ssize_t bytes_read = read(client_fd, &message, sizeof(message));
    if (bytes_read <= 0) {
      if (bytes_read == -1) {
        fprintf(stderr, "Failed to read from client_fd");
      } else {
        fprintf(stderr, "Client disconnected: %d\n", client_fd);
      }
      remove_client(subscription_table,client_fd);
      close(client_fd);
      break;
    }

    switch (message.opcode) {
      case OP_CODE_SUBSCRIBE:
        resp_fd = open(message.data.connect.resp_pipe, O_WRONLY);
        if (resp_fd == -1) {
          fprintf(stderr, "Failed to open client response pipe for subscribe");
        } else {
          int success = 0;
          if (subscribe_client(subscription_table, client_fd, 
                message.data.key,message.data.connect.notif_pipe) != 0 ){
            success = 1;
          }
          
          if (write(resp_fd, &success, sizeof(success)) == -1) {
            fprintf(stderr, "Failed to write subscribe to client");
          }
          print_hash_table(subscription_table);
          close(resp_fd);
        }
        break;

      case OP_CODE_UNSUBSCRIBE:
        resp_fd = open(message.data.connect.resp_pipe, O_WRONLY);
        if (resp_fd == -1) {
          fprintf(stderr, 
          "Failed to open client response pipe for unsubscribe acknowledgment");
        } else {
          int success = 0;
          if (unsubscribe_client(subscription_table, client_fd, 
                                  message.data.key) != 0){
            success = 1;
          }
          if (write(resp_fd, &success, sizeof(success)) == -1) {
            fprintf(stderr, 
            "Failed to write unsubscribe acknowledgment to client");
          }
          print_hash_table(subscription_table);
          close(resp_fd);
        }
        break;

      case OP_CODE_DISCONNECT:
        resp_fd = open(message.data.connect.resp_pipe, O_WRONLY);
        if (resp_fd == -1) {
          fprintf(stderr, 
          "Failed to open client response pipe for disconnect acknowledgment");
        } else {
          char success = 0; // Indicate successful disconnection
          
          if (write(resp_fd, &success, sizeof(success)) == -1) {
            fprintf(stderr, 
            "Failed to write disconnection acknowledgment to client");
          }
          close(resp_fd);
        }
        remove_client(subscription_table,client_fd);//Remover cliente da hashtable de clientes
        close(client_fd);
        return;
      default:
        fprintf(stderr, "Unknown opcode received: %d\n", message.opcode);
        break;
    }
  }
}

//void add_client(int client_fd){
//  for (int i = 0; i < MANAGING_THREADS; i++){
//    if (clients[i] = 0)
//      clients[i] = client_fd;
//  }
//}

//void del_client(int client_fd){
//  for (int i = 0; i < MANAGING_THREADS; i++){
//    if (clients[i] = client_fd)
//      clients[i] = 0;
//  }
//}

void cleanup_and_disconnect_clients(){

  //for (int i = 0; i < MANAGING_THREADS; i++){
  //  clients[i] 
//
  //}
}

void *client_handler() {

  sigemptyset(&set);
  sigaddset(&set, SIGUSR1);
  if (pthread_sigmask(SIG_BLOCK, &set, NULL) != 0){
    perror("sigmask\n");
  }

  while (1) {
    int client_fd;

    sem_wait(&filled_slots);

    // Dequeue a client connection
    if (queue_dequeue(&client_queue, &client_fd) != 0) {
      fprintf(stderr, "Failed to dequeue a client.\n");
      continue; // Try to dequeue again
    }
    
    // Process further commands from the client
    process_client_commands(client_fd);

    sem_post(&empty_slots);
  }
  pthread_exit(NULL);
}

void *init_server_pipes() {

  unlink(registration_pipe_name);
  if (mkfifo(registration_pipe_name, 0777) == -1) {
    fprintf(stderr, "Failed to create server registration pipe");
    pthread_exit(NULL);
  }


  signal(SIGUSR1, handle_sigusr1);

  // Open the FIFO in O_RDWR mode to prevent EOF issues
  int server_fd = open(registration_pipe_name, O_RDONLY);
  if (server_fd == -1) {
    fprintf(stderr, "Failed to open server registration pipe");
    unlink(registration_pipe_name);
    pthread_exit(NULL);
  }

  while (1) {

    if (sigusr1_received) {
      //cleanup_and_disconnect_clients();
      sigusr1_received = 0;
    }


    struct {
      char opcode;
      union {
        struct {
          char req_pipe[40];
          char resp_pipe[40];
          char notif_pipe[40];
        } connect;
        char key[40]; // Field for subscribe/unsubscribe
      } data;
    } message;

    // Read from the server pipe
    ssize_t bytes_read = read(server_fd, &message, sizeof(message));
    if (bytes_read <= 0) {
      if (bytes_read == 0) {
        // No data: Reopen the pipe in case the writer has closed it
        close(server_fd);
        server_fd = open(registration_pipe_name, O_RDWR);
        if (server_fd == -1) {
          fprintf(stderr, "Failed to reopen server registration pipe");
          break;
        }
      } else {
        fprintf(stderr, "Error reading from registration pipe");
        break;
      }
      continue; // Retry after reopening
    }

    if (message.opcode == OP_CODE_CONNECT) {
      int resp_fd = -1;
      int success = 0; // Default to success (0)

      sem_wait(&empty_slots);

      // Open the client's request pipe
      int client_fd = open(message.data.connect.req_pipe, O_RDONLY);
      if (client_fd == -1) {
        fprintf(stderr, "Failed to open client request pipe");
        success = 1; // Indicate failure
      } else {
        printf("Client connected: %d\n",client_fd);
        // Enqueue the client connection
        if (queue_enqueue(&client_queue, client_fd) != 0) {
          fprintf(stderr, "Queue is full. Cannot accept client.\n");
          close(client_fd);
          success = 1; // Indicate failure
        }
      }
      // Open the response pipe to send acknowledgment
      resp_fd = open(message.data.connect.resp_pipe, O_WRONLY);
      if (resp_fd == -1) {
        fprintf(stderr, "Failed to open client response pipe");
        if (client_fd != -1) {
          close(client_fd);
        }
        continue;
      }

      // Write acknowledgment to the client
      if (write(resp_fd, &success, sizeof(success)) == -1) {
        fprintf(stderr, "Failed to write to client response pipe");
      }
      close(resp_fd);

      sem_post(&filled_slots);
    } else {
      fprintf(stderr, "Unexpected opcode received: %d\n", message.opcode);
    }
  }

  close(server_fd);
  unlink(registration_pipe_name);
  pthread_exit(NULL);
}





static void dispatch_threads(DIR *dir) {
  pthread_t host_thread;
  pthread_t *job_threads = malloc(max_threads * sizeof(pthread_t));
  pthread_t *client_threads = malloc(MANAGING_THREADS * sizeof(pthread_t));

  if (job_threads == NULL || client_threads == NULL) {
    fprintf(stderr, "Failed to allocate memory for threads\n");
    return;
  }

  struct SharedData thread_data = {dir, jobs_directory, PTHREAD_MUTEX_INITIALIZER};

  // Create host thread to handle client connections
  if (pthread_create(&host_thread, NULL, init_server_pipes, NULL) != 0) {
    fprintf(stderr, "Failed to create host thread\n");
    free(job_threads);
    free(client_threads);
    return;
  }

  // Create threads for processing job files
  for (size_t i = 0; i < max_threads; i++) {
    if (pthread_create(&job_threads[i], NULL, get_file, (void *)&thread_data) != 0) {
      fprintf(stderr, "Failed to create job threads\n");
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(job_threads);
      free(client_threads);
      return;
    }
  }

  // Create threads for handling client connections
  for (size_t i = 0; i < MANAGING_THREADS; i++) {
    if (pthread_create(&client_threads[i], NULL, client_handler, NULL) != 0) {
      fprintf(stderr, "Failed to create client threads\n");
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(job_threads);
      free(client_threads);
      return;
    }
  }

  // Join host thread
  if (pthread_join(host_thread, NULL) != 0) {
    fprintf(stderr, "Failed to join host thread\n");
    pthread_mutex_destroy(&thread_data.directory_mutex);
    free(job_threads);
    free(client_threads);
    return;
  }

  // Join job threads
  for (size_t i = 0; i < max_threads; i++) {
    if (pthread_join(job_threads[i], NULL) != 0) {
      fprintf(stderr, "Failed to join job threads\n");
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(job_threads);
      free(client_threads);
      return;
    }
  }

  // Join client threads
  for (size_t i = 0; i < MANAGING_THREADS; i++) {
    if (pthread_join(client_threads[i], NULL) != 0) {
      fprintf(stderr, "Failed to join client threads\n");
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(job_threads);
      free(client_threads);
      return;
    }
  }

  if (pthread_mutex_destroy(&thread_data.directory_mutex) != 0) {
    fprintf(stderr, "Failed to destroy directory_mutex\n");
  }

  free(job_threads);
  free(client_threads);
}







int main(int argc, char **argv) {
  if (argc < 5) {
    write_str(STDERR_FILENO, "Usage: ");
    write_str(STDERR_FILENO, argv[0]);
    write_str(STDERR_FILENO, " <jobs_dir>");
    write_str(STDERR_FILENO, " <max_threads>");
    write_str(STDERR_FILENO, " <max_backups>");
    write_str(STDERR_FILENO, " <FIFO_de_registo>\n");
    return 1;
  }

  pid_t server_pid = getpid();
  printf("Server PID: %d\n", server_pid);

  jobs_directory = argv[1];

  char *endptr;
  max_backups = strtoul(argv[3], &endptr, 10);

  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_proc value\n");
    return 1;
  }

  max_threads = strtoul(argv[2], &endptr, 10);

  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_threads value\n");
    return 1;
  }

  if (max_backups <= 0) {
    write_str(STDERR_FILENO, "Invalid number of backups\n");
    return 0;
  }

  if (max_threads <= 0) {
    write_str(STDERR_FILENO, "Invalid number of threads\n");
    return 0;
  }

  if (kvs_init()) {
    write_str(STDERR_FILENO, "Failed to initialize KVS\n");
    return 1;
  }

  //INFO: Inicializar a hashtable do clientes-----------------------------------
  if (subscription_table_init() != 0) {
    fprintf(stderr, "Failed to initialize subscription table.\n");
    kvs_terminate();
    return 1;
  }

  sem_init(&empty_slots, 0, MANAGING_THREADS);
  sem_init(&filled_slots, 0, 0);


  registration_pipe_name = argv[4];
  if (queue_init(&client_queue, MANAGING_THREADS) != 0) {
    fprintf(stderr, "Failed to initialize client queue.\n");
    return 1;
  }

  DIR *dir = opendir(argv[1]);
  if (dir == NULL) {
    fprintf(stderr, "Failed to open directory: %s\n", argv[1]);
    return 0;
  }

  dispatch_threads(dir);

  if (closedir(dir) == -1) {
    fprintf(stderr, "Failed to close directory\n");
    return 0;
  }

  while (active_backups > 0) {
    wait(NULL);
    active_backups--;
  }

  subscription_table_destroy();
  kvs_terminate();

  return 0;
}
