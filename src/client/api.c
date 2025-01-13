#include "api.h"
#include <stdio.h>
#include <fcntl.h>
#include "src/common/constants.h"
#include "src/common/protocol.h"
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdbool.h>

int req_fd = -1;
int res_fd = -1;
int not_fd = -1;
int server_fd = -1;

char req_pipe_path[256];
char resp_pipe_path[256];
char notif_pipe_path[256];
char server_pipe_path[256];



//------------------------------------------------------------------------------

void cleanup_pipes() {
  if (req_fd >= 0){
    close(req_fd);
  }
  if (res_fd >= 0) {
    close(res_fd);
  } 
  if (not_fd >= 0){
    close(not_fd);
  }
  unlink(req_pipe_path);
  unlink(resp_pipe_path);
  unlink(notif_pipe_path);
}



void print_server_response(int server_response, const char *operation) {
    printf("Server returned %d for operation: %s\n", server_response, operation);
}

void handle_response(int mode, int server_response) {
    switch (mode) {
        case OP_CODE_CONNECT:
            print_server_response(server_response, "connect");
            break;
        case OP_CODE_DISCONNECT:
            print_server_response(server_response, "disconnect");
            break;
        case OP_CODE_SUBSCRIBE:
            print_server_response(server_response, "subscribe");
            break;
        case OP_CODE_UNSUBSCRIBE:
            print_server_response(server_response, "unsubscribe");
            break;
        default:
            printf("Unknown operation mode: %d\n", mode);
            break;
    }
}

int send_message(int mode, const char *key, bool use_req_fd) {
    int pipe_fd;

    if (use_req_fd) {
        pipe_fd = req_fd;
    } else {
        pipe_fd = server_fd;
    }

    if (pipe_fd < 0) {
        fprintf(stderr, "Invalid pipe descriptor\n");
        return 1;
    }

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

    size_t message_size;

    // Handle different modes
    if (mode == OP_CODE_CONNECT || mode == OP_CODE_DISCONNECT) {
        // Connect or Disconnect Message
        message.opcode = mode;
        strncpy(message.data.connect.req_pipe, req_pipe_path, sizeof(message.data.connect.req_pipe));
        strncpy(message.data.connect.resp_pipe, resp_pipe_path, sizeof(message.data.connect.resp_pipe));
        strncpy(message.data.connect.notif_pipe, notif_pipe_path, sizeof(message.data.connect.notif_pipe));
        message_size = sizeof(message);
    } else if (mode == OP_CODE_SUBSCRIBE || mode == OP_CODE_UNSUBSCRIBE) {
        // Subscribe or Unsubscribe Message
        message.opcode = mode;
        strncpy(message.data.connect.req_pipe, req_pipe_path, sizeof(message.data.connect.req_pipe));
        strncpy(message.data.connect.resp_pipe, resp_pipe_path, sizeof(message.data.connect.resp_pipe));
        strncpy(message.data.connect.notif_pipe, notif_pipe_path, sizeof(message.data.connect.notif_pipe));
        if (key == NULL || strlen(key) >= sizeof(message.data.key)) {
            fprintf(stderr, "Invalid or missing key\n");
            return 1;
        }
        strncpy(message.data.key, key, sizeof(message.data.key));
        message_size = sizeof(message);
    } else {
        fprintf(stderr, "Invalid operation code\n");
        return 1;
    }

    // Write the message to the selected pipe
    if (write(pipe_fd, &message, message_size) != (ssize_t)message_size) {
        fprintf(stderr, "Failed to send message\n");
        return 1;
    }

    // Read the response from the server
    int server_response;
    ssize_t bytes_read = read(res_fd, &server_response, sizeof(server_response));
    if (bytes_read <= 0) {
        if (bytes_read == -1) {
            fprintf(stderr, "Failed to read server response\n");
        } else {
            fprintf(stderr, "Server closed the connection\n");
        }
        return 1;
    }

    handle_response(mode, server_response);

    if (server_response != 0) {
        return 1;
    }

    return 0;
}




int open_pipes(){

  req_fd = open(req_pipe_path,  O_RDWR);
  if (req_fd < 0){
    fprintf(stderr, "Failed to open request pipe\n");
    return 1;
  }
  res_fd = open(resp_pipe_path, O_RDWR);
  if (res_fd < 0){
    fprintf(stderr, "Failed to open response pipe\n");
    close(req_fd);
    return 1;
  }
  not_fd = open(notif_pipe_path, O_RDONLY | O_NONBLOCK);

  if (not_fd < 0 ){
    fprintf(stderr, "Failed to open notifications pipe\n");
    close(req_fd);
    close(res_fd);
    return 1;
  }
  return 0;
}



int kvs_connect(char const *req_pipe_path_arg, char const *resp_pipe_path_arg,
                char const *server_pipe_path_arg, char const *notif_pipe_path_arg) {
  

  strncpy(req_pipe_path, req_pipe_path_arg, sizeof(req_pipe_path) - 1);
  strncpy(resp_pipe_path, resp_pipe_path_arg, sizeof(resp_pipe_path) - 1);
  strncpy(notif_pipe_path, notif_pipe_path_arg, sizeof(notif_pipe_path) - 1);
  strncpy(server_pipe_path,server_pipe_path_arg, sizeof(server_pipe_path)-1);
  
  unlink(req_pipe_path);
  unlink(resp_pipe_path);
  unlink(notif_pipe_path);

  if (mkfifo(req_pipe_path, 0644) != 0) {
    fprintf(stderr, "Failed to create request pipe");
    return 1;
  }

  if (mkfifo(resp_pipe_path, 0644) != 0) {
    fprintf(stderr, "Failed to create response pipe");
    unlink(req_pipe_path);
    return 1;
  }

  if (mkfifo(notif_pipe_path, 0644) != 0) {
    fprintf(stderr, "Failed to create notifications pipe");
    unlink(req_pipe_path);
    unlink(resp_pipe_path);
    return 1;
  }
  if (open_pipes() != 0) {
    cleanup_pipes();
    return 1;
  }
  // Send the connection message to the server
  server_fd = open(server_pipe_path, O_WRONLY);
  if (server_fd < 0) {
    fprintf(stderr, "Failed to open server pipe\n");
    cleanup_pipes();
    return 1;
  }
  if(send_message(OP_CODE_CONNECT,NULL,false) == 1){
    cleanup_pipes();
    return 1;
  }
  
  return 0;
}



int kvs_disconnect(void) {
  if (send_message(OP_CODE_DISCONNECT,NULL,true) == 1){
    return 1;
  }
  cleanup_pipes();
  if (server_fd >= 0) {
    close(server_fd);
    server_fd = -1;
  }
  req_fd = res_fd = not_fd = -1;
  return 0;
}

int kvs_subscribe(const char *key) {
  // send subscribe message to request pipe and wait for response in response
  // pipe
  return send_message(OP_CODE_SUBSCRIBE,key,true); 
}

int kvs_unsubscribe(const char *key) {
  // send unsubscribe message to request pipe and wait for response in response
  // pipe
  return send_message(OP_CODE_UNSUBSCRIBE,key,true);
}
