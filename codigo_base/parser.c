#include "parser.h"
#include <dirent.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include "constants.h"

static int read_string(int fd, char *buffer, size_t max) {
  ssize_t bytes_read;
  char ch;
  size_t i = 0;
  int value = -1;

  while (i < max) {
    bytes_read = read(fd, &ch, 1);

    if (bytes_read <= 0) {
        return -1;
    }

    if (ch == ' ') {
      return -1;
    }

    if (ch == ',') {
      value = 0;
      break;
    }
    else if (ch == ')') {
      value = 1;
      break;
    }
    else if (ch == ']') {
      value = 2;
      break;
    }

    buffer[i++] = ch;
  }

  buffer[i] = '\0';

  return value;
}

static int read_uint(int fd, unsigned int *value, char *next) {
  char buf[16];

  int i = 0;
  while (1) {
    if (read(fd, buf + i, 1) == 0) {
      *next = '\0';
      break;
    }

    *next = buf[i];

    if (buf[i] > '9' || buf[i] < '0') {
      buf[i] = '\0';
      break;
    }

    i++;
  }

  unsigned long ul = strtoul(buf, NULL, 10);

  if (ul > UINT_MAX) {
    return 1;
  }

  *value = (unsigned int)ul;

  return 0;
}

static void cleanup(int fd) {
  char ch;
  while (read(fd, &ch, 1) == 1 && ch != '\n')
    ;
}


enum Command get_next(const char *line) {
    if (line == NULL || *line == '\0') {
        return CMD_EMPTY;
    }

    switch (line[0]) {
        case 'W':
            if (strncmp(line, "WAIT ", 5) == 0) {
                return CMD_WAIT;
            } else if (strncmp(line, "WRITE ", 6) == 0) {
                return CMD_WRITE;
            }
            return CMD_INVALID;

        case 'R':
            if (strncmp(line, "READ ", 5) == 0) {
                return CMD_READ;
            }
            return CMD_INVALID;

        case 'D':
            if (strncmp(line, "DELETE ", 7) == 0) {
                return CMD_DELETE;
            }
            return CMD_INVALID;

        case 'S':
            if (strncmp(line, "SHOW", 4) == 0 && (line[4] == '\0' || line[4] == '\n')) {
                return CMD_SHOW;
            }
            return CMD_INVALID;

        case 'B':
            if (strncmp(line, "BACKUP", 6) == 0 && (line[6] == '\0' || line[6] == '\n')) {
                return CMD_BACKUP;
            }
            return CMD_INVALID;

        case 'H':
            if (strncmp(line, "HELP", 4) == 0 && (line[4] == '\0' || line[4] == '\n')) {
                return CMD_HELP;
            }
            return CMD_INVALID;

        case '#':
            return CMD_EMPTY;

        case '\n':
            return CMD_EMPTY;

        default:
            return CMD_INVALID;
    }
}

int parse_pair(int fd, char *key, char *value) {
  if (read_string(fd, key, MAX_STRING_SIZE) != 0) {
    cleanup(fd);
    return 0;
  }

  if (read_string(fd, value, MAX_STRING_SIZE) != 1) {
    cleanup(fd);
    return 0;
  }

  return 1;
}

size_t parse_write(int fd, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE], size_t max_pairs, size_t max_string_size) {
  char ch;

  if (read(fd, &ch, 1) != 1 || ch != '[') {
    printf("Expected '[', but got '%c'\n", ch);
    //cleanup(fd);
    return 0;
  }

  if (read(fd, &ch, 1) != 1 || ch != '(') {
    cleanup(fd);
    return 0;
  }

  size_t num_pairs = 0;
  char key[max_string_size];
  char value[max_string_size];
  while (num_pairs < max_pairs) {
    if(parse_pair(fd, key, value) == 0) {
      cleanup(fd);
      return 0;
    }

    strcpy(keys[num_pairs], key);
    strcpy(values[num_pairs++], value);

    if (read(fd, &ch, 1) != 1 || (ch != '(' && ch != ']')) {
      cleanup(fd);
      return 0;
    }

    if (ch == ']') {
      break;
    }
  }

  if (num_pairs == max_pairs) {
    cleanup(fd);
    return 0;
  }

  if (read(fd, &ch, 1) != 1 || (ch != '\n' && ch != '\0')) {
    cleanup(fd);
    return 0;
  }

  return num_pairs;
}

size_t parse_read_delete(int fd, char keys[][MAX_STRING_SIZE], size_t max_keys, size_t max_string_size) {
  char ch;

  if (read(fd, &ch, 1) != 1 || ch != '[') {
    cleanup(fd);
    return 0;
  }

  size_t num_keys = 0;
  char key[max_string_size];
  while (num_keys < max_keys) {
    int output = read_string(fd, key, max_string_size);
    if(output < 0 || output == 1) {
      cleanup(fd);
      return 0;
    }

    strcpy(keys[num_keys++], key);

    if (output == 2){
      break;
    }
  }

  if (num_keys == max_keys) {
    cleanup(fd);
    return 0;
  }

  if (read(fd, &ch, 1) != 1 || (ch != '\n' && ch != '\0')) {
    cleanup(fd);
    return 0;
  }

  return num_keys;
}

int parse_wait(int fd, unsigned int *delay, unsigned int *thread_id) {
  char ch;

  if (read_uint(fd, delay, &ch) != 0) {
    cleanup(fd);
    return -1;
  }

  if (ch == ' ') {
    if (thread_id == NULL) {
      cleanup(fd);
      return 0;
    }

    if (read_uint(fd, thread_id, &ch) != 0 || (ch != '\n' && ch != '\0')) {
      cleanup(fd);
      return -1;
    }

    return 1;
  } else if (ch == '\n' || ch == '\0') {
    return 0;
  } else {
    cleanup(fd);
    return -1;
  }
}

