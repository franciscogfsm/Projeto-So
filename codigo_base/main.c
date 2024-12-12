#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>
#include "constants.h"
#include "parser.h"
#include <fcntl.h>
#include "operations.h"
#include <string.h>
#include <sys/wait.h>
#include <pthread.h>

///home/kali/Desktop/Projeto-So/tests-public/jobs 2
///home/kali/Desktop/Projeto-So/codigo_base/tests-public/jobs 2
///home/kali/Desktop/Projeto-So/jobs/ 2

typedef struct {
    char **Job_paths;
    int num_jobs;
    const char *buffer;
    int MAX_BACKUPS;
    pthread_mutex_t *mutex_jobs;
    pthread_mutex_t *mutex_hashtable;
    int *current_job;
    int *active_backups;
} thread_args_t;


void *job_working(void *args){
    thread_args_t *j_args = (thread_args_t *)args;

    char file_out[512];
    while(1){
        int job_index;

        //Mutex lock for avoiding threads entering the same job
        pthread_mutex_lock(j_args->mutex_jobs);
        if (*j_args -> current_job >= j_args -> num_jobs){
            pthread_mutex_unlock(j_args->mutex_jobs);
            break;
        }
        job_index = *(j_args -> current_job);
        (*(j_args->current_job))++;
        pthread_mutex_unlock(j_args -> mutex_jobs);

        //While cycle here
        int Number_bck_file=0;
        char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
        char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
        unsigned int delay;
        size_t num_pairs;
    
        int fd = open(j_args -> Job_paths[job_index], O_RDONLY);
        if (fd == -1) {
            fprintf(stderr, "Failed to open File\n");
            continue;
        }
        char *file_name = get_file_name(j_args -> Job_paths[job_index]);
        snprintf(file_out, sizeof(file_out), "%s/%s.out", j_args-> buffer, file_name);
        free(file_name);
        int fd_out = open(file_out, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd_out == -1) {
            fprintf(stderr, "Failed to Create File\n");
            close(fd);
            continue;
        }
        //While is here
        while(1){
            enum Command cmd = get_next(fd);
            
            if (cmd == EOC) {
                break;
            }
            switch (cmd) {
                case CMD_WRITE:
                    num_pairs = parse_write(fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                    if (num_pairs == 0) {
                        continue;
                    }
                    line_locker(num_pairs, keys);
                    
                    if (kvs_write(num_pairs, keys, values)) {
                        fprintf(stderr, "Failed to write pair\n");
                    }
                    line_unlocker(num_pairs, keys);
                    break;

                case CMD_READ:
                    num_pairs = parse_read_delete(fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                    if (num_pairs == 0) {
                        fprintf(stderr, "Invalid command. See HELP for usage\n");
                        continue;
                    }
                    line_locker(num_pairs, keys);
                    if (kvs_read(num_pairs,keys,fd_out)) {
                        fprintf(stderr, "Failed to read pair\n");
                    }
                    line_unlocker(num_pairs, keys);
                    break;                            

                case CMD_DELETE:
                    num_pairs = parse_read_delete(fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                    if (num_pairs == 0) {
                        fprintf(stderr, "Invalid command. See HELP for usage\n");
                        continue;
                    }
                    line_locker(num_pairs, keys);
                    if (kvs_delete(num_pairs, keys,fd_out)) {
                        fprintf(stderr, "Failed to delete pair\n");
                    }
                    line_unlocker(num_pairs, keys);
                    break;

                case CMD_SHOW:
                    pthread_mutex_lock(j_args -> mutex_hashtable);
                    kvs_show(fd_out);
                    pthread_mutex_unlock(j_args -> mutex_hashtable);
                    break;

                case CMD_WAIT:
                    if (parse_wait(fd, &delay, NULL) == -1) {
                        fprintf(stderr, "Invalid command. See HELP for usage\n");
                        continue;
                    }
                    if (delay > 0) {
                        write(fd_out, "Waiting...\n", sizeof("Waiting...\n") - 1);
                        kvs_wait(delay);
                    }
                    break;

                case CMD_BACKUP:
                    pthread_mutex_lock(j_args -> mutex_hashtable);
                    (*(j_args->active_backups))++;
                    if (*(j_args -> active_backups) > j_args -> MAX_BACKUPS){
                        wait(NULL);
                        (*(j_args->active_backups))--;
                    }
                    Number_bck_file++;
                    if (kvs_backup(j_args -> Job_paths[job_index], j_args -> buffer, Number_bck_file)) {
                        fprintf(stderr, "Failed to perform backup.\n");
                    }
                    pthread_mutex_unlock(j_args -> mutex_hashtable);
                    break;

                case CMD_INVALID:
                    fprintf(stderr, "Invalid command. See HELP for usage\n");
                    break;

                case CMD_HELP:
                    printf(
                        "Available commands:\n"
                        "  WRITE [(key,value)(key2,value2),...]\n"
                        "  READ [key,key2,...]\n"
                        "  DELETE [key,key2,...]\n"
                        "  SHOW\n"
                        "  WAIT <delay_ms>\n"
                        "  BACKUP\n" // Not implemented
                        "  HELP\n"
                    );
                    break;

                case CMD_EMPTY:
                    break;

                case EOC:
                    kvs_terminate();
                    exit(0);
                    fprintf(stderr, "Invalid command. See HELP for usage\n");
            }
        }
        close(fd);
        close(fd_out);
    }

    return NULL;
}





int main(int argc, char *argv[]) {
    
    if (argc != 4) {
        fprintf(stderr, "Wrong usage");
        return 1;
    }
    
    if (kvs_init()) {
        fprintf(stderr, "Failed to initialize KVS\n");
        return 1;
    }
    

    const char *buffer = argv[1];
    int MAX_BACKUPS = atoi(argv[2]);
    int MAX_THREADS = atoi(argv[3]);

    DIR *dir = opendir(buffer);
    if (dir == NULL) {
        fprintf(stderr, "Opendir failed\n");
        return 1;
    }
    //Find the paths for all of the job files and put it in the array
    int num_jobs;
    char **Job_paths = find_jobs(dir, buffer, &num_jobs);
    if (Job_paths == NULL) {
        return 1;
    }
    
    

    pthread_mutex_t mutex_jobs = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t mutex_hashtable = PTHREAD_MUTEX_INITIALIZER;
    int current_job = 0;
    int active_backups = 0;
    pthread_t threads[MAX_THREADS];
    thread_args_t args = {
        .Job_paths = Job_paths,
        .num_jobs = num_jobs,
        .buffer = buffer,
        .MAX_BACKUPS = MAX_BACKUPS,
        .mutex_jobs = &mutex_jobs,
        .mutex_hashtable = &mutex_hashtable,
        .current_job = &current_job,
        .active_backups = &active_backups
    };
    
    if (MAX_THREADS> num_jobs){
        MAX_THREADS = num_jobs;
    }
    
    for (int i = 0; i < MAX_THREADS; i++) {
        int ret = pthread_create(&threads[i], NULL, job_working, &args);
        if (ret != 0) {
            fprintf(stderr, "Failed to create thread %d\n", i);
        }
    }
    
    for (int i = 0; i < MAX_THREADS; i++) {
        int ret = pthread_join(threads[i], NULL);
        if (ret != 0) {
            fprintf(stderr, "Failed to join thread %d\n", i);
        }
    }

    closedir(dir);
    free_job_paths(Job_paths, num_jobs);
    kvs_terminate();
    printf("Finished Sucess\n");
    return 0;
}