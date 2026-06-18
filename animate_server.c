
//#include <animate/animate.h>

#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <pthread.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <ctype.h>

#define MAX_COMMAND 256
#define MAX_USERNAME 32
#define QUEUE_SIZE 1024

uint64_t global_order_counter = 0;
uint64_t next_to_respond = 0;
pthread_mutex_t order_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t order_cond = PTHREAD_COND_INITIALIZER;

typedef struct {
    uint64_t order_id;
    pid_t client_pid;
    int fd_s2c;
    char command[MAX_COMMAND];
} rpc_task_t;

// Task Queue
rpc_task_t task_queue[QUEUE_SIZE];
int q_head = 0, q_tail = 0, q_count = 0;
pthread_mutex_t q_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t q_not_empty = PTHREAD_COND_INITIALIZER;
pthread_cond_t q_not_full = PTHREAD_COND_INITIALIZER;

// Signal self-pipe for safely handling SIGUSR1
int sigusr1_pipe[2];

void trim_whitespace(char *str) {
    char *end;
    while(isspace((unsigned char)*str)) str++;
    if(*str == 0) return;
    end = str + strlen(str) - 1;
    while(end > str && isspace((unsigned char)*end)) end--;
    end[1] = '\0';
}

// Check users.txt for authorization
int authenticate_user(const char* username, int* out_balance) {
    FILE* f = fopen("users.txt", "r");
    if (!f) return -1; 
    
    char line[256];
    while (fgets(line, sizeof(line), f)) {
        char file_user[128];
        int balance;
        
        char *ptr = line;
        while(isspace((unsigned char)*ptr)) ptr++;
        
        if (sscanf(ptr, "%127s %d", file_user, &balance) == 2) {
            if (strcmp(file_user, username) == 0) {
                *out_balance = balance;
                fclose(f);
                return 1; 
            }
        }
    }
    fclose(f);
    return 0; 
}

// Cleanup thread to handle the 1-second delay without blocking
void* reject_cleanup_thread(void* arg) {
    pid_t pid = *(pid_t*)arg;
    free(arg);
    
    sleep(1); 
    
    char fifo_c2s[128], fifo_s2c[128];
    snprintf(fifo_c2s, sizeof(fifo_c2s), "FIFO_C2S_%d", pid);
    snprintf(fifo_s2c, sizeof(fifo_s2c), "FIFO_S2C_%d", pid);
    
    unlink(fifo_c2s);
    unlink(fifo_s2c);
    return NULL;
}

// worker thread pool
void* worker_thread(void* arg) {
    (void)arg;
    while (1) {
        // Grab a task from the queue
        pthread_mutex_lock(&q_mutex);
        while (q_count == 0) {
            pthread_cond_wait(&q_not_empty, &q_mutex);
        }
        rpc_task_t task = task_queue[q_head];
        q_head = (q_head + 1) % QUEUE_SIZE;
        q_count--;
        pthread_cond_signal(&q_not_full);
        pthread_mutex_unlock(&q_mutex);

        // Process logic (in parallel as per starvation)
        char response[MAX_COMMAND] = {0};
        char username[MAX_USERNAME + 1] = {0};
        int is_reject = 0;

        if (sscanf(task.command, "Login %32s", username) == 1) {
            int balance = 0;
            int auth_status = authenticate_user(username, &balance);
            
            if (auth_status == 1 && balance > 0) {
                snprintf(response, sizeof(response), "%d\n", balance);
            } else if (auth_status == 1 && balance <= 0) {
                snprintf(response, sizeof(response), "Reject BALANCE\n");
                is_reject = 1;
            } else {
                snprintf(response, sizeof(response), "Reject UNAUTHORISED\n");
                is_reject = 1;
            }
        } else {
            // Unrecognized command / dummy placeholder for future RPCs
            snprintf(response, sizeof(response), "ERROR Unknown Command\n");
        }

        // Strict Ordering Gate (Wait for our turn to respond)
        pthread_mutex_lock(&order_mutex);
        while (task.order_id != next_to_respond) {
            pthread_cond_wait(&order_cond, &order_mutex);
        }

        // Send the response exactly in order
        write(task.fd_s2c, response, strlen(response));
        
        // Update gate and wake up peers
        next_to_respond++;
        pthread_cond_broadcast(&order_cond);
        pthread_mutex_unlock(&order_mutex);

        // Post-processing (Cleanup if rejected)
        if (is_reject) {
            pid_t* pid_ptr = malloc(sizeof(pid_t));
            *pid_ptr = task.client_pid;
            pthread_t cl_thread;
            pthread_create(&cl_thread, NULL, reject_cleanup_thread, pid_ptr);
            pthread_detach(cl_thread);
        }
    }
    return NULL;
}

// client reader thread
void* client_reader_thread(void* arg) {
    pid_t client_pid = *(pid_t*)arg;
    free(arg);

    char fifo_c2s[128], fifo_s2c[128];
    snprintf(fifo_c2s, sizeof(fifo_c2s), "FIFO_C2S_%d", client_pid);
    snprintf(fifo_s2c, sizeof(fifo_s2c), "FIFO_S2C_%d", client_pid);

    int fd_c2s = open(fifo_c2s, O_RDONLY);
    int fd_s2c = open(fifo_s2c, O_WRONLY);
    
    if (fd_c2s < 0 || fd_s2c < 0) return NULL;

    char buffer[MAX_COMMAND];
    FILE* stream_c2s = fdopen(fd_c2s, "r");

    while (fgets(buffer, sizeof(buffer), stream_c2s)) {
        trim_whitespace(buffer);
        if (strlen(buffer) == 0) continue;

        rpc_task_t new_task;
        new_task.client_pid = client_pid;
        new_task.fd_s2c = fd_s2c;
        strncpy(new_task.command, buffer, MAX_COMMAND - 1);

        // Assign strict ordering ID
        pthread_mutex_lock(&order_mutex);
        new_task.order_id = global_order_counter++;
        pthread_mutex_unlock(&order_mutex);

        // Push to queue
        pthread_mutex_lock(&q_mutex);
        while (q_count == QUEUE_SIZE) {
            pthread_cond_wait(&q_not_full, &q_mutex);
        }
        task_queue[q_tail] = new_task;
        q_tail = (q_tail + 1) % QUEUE_SIZE;
        q_count++;
        pthread_cond_signal(&q_not_empty);
        pthread_mutex_unlock(&q_mutex);
    }

    fclose(stream_c2s);
    close(fd_s2c);
    return NULL;
}

// Signal handler
void handle_sigusr1(int sig, siginfo_t *info, void *context) {
    (void)sig; (void)context;
    pid_t client_pid = info->si_pid;
    // Write client PID to self-pipe to be handled by main loop
    write(sigusr1_pipe[1], &client_pid, sizeof(pid_t));
}

int main(int argc, char *argv[]) {

    struct canvas* canvas = animate_create_canvas(100,100,0);
    animate_destroy_canvas(canvas);

    if (argc != 2) {
        fprintf(stderr, "Usage: %s <threadpool size>\n", argv[0]);
        exit(1);
    }

    int pool_size = atoi(argv[1]);
    if (pool_size < 1) pool_size = 1;


    // Setup Self-Pipe for signal handling
    if (pipe(sigusr1_pipe) == -1) {
        perror("pipe"); exit(1);
    }

    // Setup SIGUSR1 Handler
    struct sigaction sa;
    sa.sa_flags = SA_SIGINFO | SA_RESTART;
    sa.sa_sigaction = handle_sigusr1;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGUSR1, &sa, NULL);

    // Print Server PID exactly as specified
    printf("Server PID: %d\n", getpid());
    fflush(stdout);

    // Create Thread Pool
    pthread_t* workers = malloc(pool_size * sizeof(pthread_t));
    for (int i = 0; i < pool_size; i++) {
        pthread_create(&workers[i], NULL, worker_thread, NULL);
    }

    // Main Acceptor Loop
    while (1) {
        pid_t client_pid;
        if (read(sigusr1_pipe[0], &client_pid, sizeof(pid_t)) > 0) {
            
            char fifo_c2s[128], fifo_s2c[128];
            snprintf(fifo_c2s, sizeof(fifo_c2s), "FIFO_C2S_%d", client_pid);
            snprintf(fifo_s2c, sizeof(fifo_s2c), "FIFO_S2C_%d", client_pid);

            // Unlink existing and create new FIFOs
            unlink(fifo_c2s);
            unlink(fifo_s2c);
            mkfifo(fifo_c2s, 0666);
            mkfifo(fifo_s2c, 0666);

            // Send SIGUSR2 back
            kill(client_pid, SIGUSR2);

            // Spawn dedicated reader thread for this client
            pid_t* arg_pid = malloc(sizeof(pid_t));
            *arg_pid = client_pid;
            pthread_t reader_tid;
            pthread_create(&reader_tid, NULL, client_reader_thread, arg_pid);
            pthread_detach(reader_tid);
        }
    }

    free(workers);
    return 0;
}

