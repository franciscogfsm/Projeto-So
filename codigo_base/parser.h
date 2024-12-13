#ifndef KVS_PARSER_H
#define KVS_PARSER_H
#include <dirent.h>
#include <stddef.h>
#include "constants.h"

enum Command {
  CMD_WRITE,
  CMD_READ,
  CMD_DELETE,
  CMD_SHOW,
  CMD_WAIT,
  CMD_BACKUP,
  CMD_HELP,
  CMD_EMPTY,
  CMD_INVALID,
  EOC  // End of commands
};

/// Reads a line and returns the corresponding command.
/// @param fd File descriptor to read from.
/// @return The command read.
enum Command get_next(int fd);

/// Parses a WRITE command.
/// @param fd File descriptor to read from.
/// @param keys Array of keys to be written.
/// @param values Array of values to be written.
/// @param max_pairs number of pairs to be written.
/// @param max_string_size maximum size for keys and values.
/// @return 0 if the command was parsed successfully, 1 otherwise.
size_t parse_write(int fd, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE], size_t max_pairs, size_t max_string_size);

/// Parses a READ or DELETE command.
/// @param fd File descriptor to read from.
/// @param keys Array of keys to be written.
/// @param max_keys number of keys to be iread or deleted.
/// @param max_string_size maximum size for keys and values.
/// @return Number of keys read or deleted. 0 on failure.
size_t parse_read_delete(int fd, char keys[][MAX_STRING_SIZE], size_t max_keys, size_t max_string_size);

/// Parses a WAIT command.
/// @param fd File descriptor to read from.
/// @param delay Pointer to the variable to store the wait delay in.
/// @param thread_id Pointer to the variable to store the thread ID in. May not be set.
/// @return 0 if no thread was specified, 1 if a thread was specified, -1 on error.
int parse_wait(int fd, unsigned int *delay, unsigned int *thread_id);

/// Compares two job paths.
/// @param a Pointer to the first job path.
/// @param b Pointer to the second job path.
/// @return An integer less than, equal to, or greater than zero if the first argument 
/// is considered to be respectively less than, equal to, or greater than the second.
int compare_job_paths(const void *a, const void *b);

///Frees the memory allocated for job paths.
/// @param Job_paths Array of job paths to be freed.
/// @param num_jobs Number of job paths in the array.
void free_job_paths(char **Job_paths, int num_jobs);


/// Finds all job files in a directory.
/// @param dir Pointer to the directory to search.
/// @param buffer Path to the directory.
/// @param num_jobs Pointer to an integer containing the number of jobs in the directory.
/// @return An array of strings containing the paths to the job files.
char** find_jobs(DIR *dir, const char *buffer, int *num_jobs);

/// Extracts the file name from a full path.
/// @param full_path full path to the file.
/// @return A string containing the file name without path and extension.
char* get_file_name(const char *full_path);




#endif  // KVS_PARSER_H
