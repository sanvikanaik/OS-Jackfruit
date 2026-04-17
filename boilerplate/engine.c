#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

/* --- Enums --- */
typedef enum {
    CMD_SUPERVISOR = 0, CMD_START, CMD_RUN, CMD_PS, CMD_LOGS, CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0, CONTAINER_RUNNING, CONTAINER_STOPPED,
    CONTAINER_KILLED, CONTAINER_EXITED
} container_state_t;

/* --- Data Structures --- */
typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head; size_t tail; size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_pipe_fd;
} child_config_t;

typedef struct {
    int fd;
    char id[CONTAINER_ID_LEN];
    bounded_buffer_t *buf;
} producer_info_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

static supervisor_ctx_t *g_ctx = NULL;

/* --- Helper --- */
static const char *state_to_string(container_state_t state) {
    switch (state) {
    case CONTAINER_STARTING: return "starting";
    case CONTAINER_RUNNING:  return "running";
    case CONTAINER_STOPPED:  return "stopped";
    case CONTAINER_KILLED:   return "killed";
    case CONTAINER_EXITED:   return "exited";
    default:                 return "unknown";
    }
}

/* --- Bounded Buffer --- */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item) {
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);
    if (buffer->shutting_down) { pthread_mutex_unlock(&buffer->mutex); return -1; }
    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;
    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item) {
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex); return -1;
    }
    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;
    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer) {
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer) {
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

/* --- Logging Thread (consumer) --- */
void *logging_thread(void *arg) {
    bounded_buffer_t *buffer = (bounded_buffer_t *)arg;
    log_item_t item;
    mkdir(LOG_DIR, 0755);
    while (bounded_buffer_pop(buffer, &item) == 0) {
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);
        int fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd >= 0) { write(fd, item.data, item.length); close(fd); }
    }
    return NULL;
}

/* --- Producer Thread (reads from pipe, pushes to buffer) --- */
void *producer_thread(void *arg) {
    producer_info_t *info = (producer_info_t *)arg;
    char buf[LOG_CHUNK_SIZE];
    int n;
    while ((n = read(info->fd, buf, sizeof(buf))) > 0) {
        log_item_t item;
        item.length = n;
        strncpy(item.container_id, info->id, CONTAINER_ID_LEN);
        memcpy(item.data, buf, n);
        bounded_buffer_push(info->buf, &item);
    }
    close(info->fd);
    free(info);
    return NULL;
}

/* --- Signal Handlers --- */
static void sigchld_handler(int sig) {
    (void)sig;
    int status; pid_t pid;
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        if (!g_ctx) continue;
        pthread_mutex_lock(&g_ctx->metadata_lock);
        for (container_record_t *c = g_ctx->containers; c; c = c->next) {
            if (c->host_pid == pid) {
                c->state = WIFEXITED(status) ? CONTAINER_EXITED : CONTAINER_KILLED;
                c->exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : 0;
                c->exit_signal = WIFSIGNALED(status) ? WTERMSIG(status) : 0;
                break;
            }
        }
        pthread_mutex_unlock(&g_ctx->metadata_lock);
    }
}

static void sigterm_handler(int sig) {
    (void)sig;
    if (g_ctx) g_ctx->should_stop = 1;
}

/* --- Child Function (runs inside container) --- */
int child_fn(void *arg) {
    child_config_t *config = (child_config_t *)arg;

    sethostname(config->id, strlen(config->id));

    dup2(config->log_pipe_fd, STDOUT_FILENO);
    dup2(config->log_pipe_fd, STDERR_FILENO);
    close(config->log_pipe_fd);

    if (chroot(config->rootfs) != 0 || chdir("/") != 0) return 1;

    mount("proc", "/proc", "proc", 0, NULL);

    if (config->nice_value != 0)
        nice(config->nice_value);

    char *exec_args[] = { config->command, NULL };
    execv(config->command, exec_args);
    return 1;
}

/* --- Launch a container --- */
static pid_t launch_container(supervisor_ctx_t *ctx, control_request_t *req) {
    int pipefds[2];
    if (pipe(pipefds) != 0) return -1;

    child_config_t *config = calloc(1, sizeof(*config));
    strncpy(config->id, req->container_id, CONTAINER_ID_LEN - 1);
    strncpy(config->rootfs, req->rootfs, PATH_MAX - 1);
    strncpy(config->command, req->command, CHILD_COMMAND_LEN - 1);
    config->nice_value = req->nice_value;
    config->log_pipe_fd = pipefds[1];

    char *stack = malloc(STACK_SIZE);
    pid_t pid = clone(child_fn, stack + STACK_SIZE,
                      CLONE_NEWPID | CLONE_NEWNS | CLONE_NEWUTS | SIGCHLD, config);
    close(pipefds[1]);

    if (pid < 0) { free(stack); free(config); close(pipefds[0]); return -1; }

    // container metadata
    container_record_t *rec = calloc(1, sizeof(*rec));
    strncpy(rec->id, req->container_id, CONTAINER_ID_LEN - 1);
    rec->host_pid = pid;
    rec->started_at = time(NULL);
    rec->state = CONTAINER_RUNNING;
    rec->soft_limit_bytes = req->soft_limit_bytes;
    rec->hard_limit_bytes = req->hard_limit_bytes;
    mkdir(LOG_DIR, 0755);
    snprintf(rec->log_path, PATH_MAX, "%s/%s.log", LOG_DIR, req->container_id);

    pthread_mutex_lock(&ctx->metadata_lock);
    rec->next = ctx->containers;
    ctx->containers = rec;
    pthread_mutex_unlock(&ctx->metadata_lock);

    // register with kernel monitor
    if (ctx->monitor_fd >= 0)
        register_with_monitor(ctx->monitor_fd, req->container_id, pid,
                              req->soft_limit_bytes, req->hard_limit_bytes);

    // start producer thread
    producer_info_t *pinfo = malloc(sizeof(*pinfo));
    pinfo->fd = pipefds[0];
    strncpy(pinfo->id, req->container_id, CONTAINER_ID_LEN);
    pinfo->buf = &ctx->log_buffer;
    pthread_t ptid;
    pthread_create(&ptid, NULL, producer_thread, pinfo);
    pthread_detach(ptid);

    return pid;
}

/* --- Supervisor --- */
static int run_supervisor(const char *rootfs) {
    supervisor_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;
    g_ctx = &ctx;

    pthread_mutex_init(&ctx.metadata_lock, NULL);
    pthread_mutex_init(&ctx.log_buffer.mutex, NULL);
    pthread_cond_init(&ctx.log_buffer.not_empty, NULL);
    pthread_cond_init(&ctx.log_buffer.not_full, NULL);

    // open kernel monitor
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr, "Warning: could not open /dev/container_monitor\n");

    // control socket
    unlink(CONTROL_PATH);
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    struct sockaddr_un addr = { .sun_family = AF_UNIX };
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);
    bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr));
    listen(ctx.server_fd, 8);

    // signals
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigchld_handler;
    sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);
    sa.sa_handler = sigterm_handler;
    sa.sa_flags = SA_RESTART;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    // logger thread
    pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx.log_buffer);

    fprintf(stderr, "Supervisor ready. rootfs=%s socket=%s\n", rootfs, CONTROL_PATH);

    // event loop
    while (!ctx.should_stop) {
        fd_set rfds; FD_ZERO(&rfds); FD_SET(ctx.server_fd, &rfds);
        struct timeval tv = { 1, 0 };
        if (select(ctx.server_fd + 1, &rfds, NULL, NULL, &tv) <= 0) continue;

        int cfd = accept(ctx.server_fd, NULL, NULL);
        if (cfd < 0) continue;

        control_request_t req;
        if (read(cfd, &req, sizeof(req)) != sizeof(req)) { close(cfd); continue; }

        control_response_t resp = {0};

        if (req.kind == CMD_START) {
            pid_t pid = launch_container(&ctx, &req);
            resp.status = pid < 0 ? -1 : 0;
            snprintf(resp.message, sizeof(resp.message),
                     pid < 0 ? "Failed to start %s" : "Started %s (pid=%d)",
                     req.container_id, pid);

        } else if (req.kind == CMD_RUN) {
            pid_t pid = launch_container(&ctx, &req);
            if (pid > 0) waitpid(pid, NULL, 0);
            resp.status = pid < 0 ? -1 : 0;
            snprintf(resp.message, sizeof(resp.message), "Done %s", req.container_id);

        } else if (req.kind == CMD_PS) {
            int off = 0;
            off += snprintf(resp.message + off, sizeof(resp.message) - off,
                            "%-16s %-8s %-10s\n", "ID", "PID", "STATE");
            pthread_mutex_lock(&ctx.metadata_lock);
            for (container_record_t *c = ctx.containers; c; c = c->next)
                off += snprintf(resp.message + off, sizeof(resp.message) - off,
                                "%-16s %-8d %-10s\n", c->id, c->host_pid,
                                state_to_string(c->state));
            pthread_mutex_unlock(&ctx.metadata_lock);
            resp.status = 0;

        } else if (req.kind == CMD_LOGS) {
            pthread_mutex_lock(&ctx.metadata_lock);
            for (container_record_t *c = ctx.containers; c; c = c->next)
                if (strcmp(c->id, req.container_id) == 0) {
                    snprintf(resp.message, sizeof(resp.message), "%s", c->log_path);
                    break;
                }
            pthread_mutex_unlock(&ctx.metadata_lock);
            resp.status = 0;

        } else if (req.kind == CMD_STOP) {
            pthread_mutex_lock(&ctx.metadata_lock);
            for (container_record_t *c = ctx.containers; c; c = c->next)
                if (strcmp(c->id, req.container_id) == 0 &&
                    c->state == CONTAINER_RUNNING) {
                    kill(c->host_pid, SIGTERM);
                    c->state = CONTAINER_STOPPED;
                    break;
                }
            pthread_mutex_unlock(&ctx.metadata_lock);
            resp.status = 0;
            snprintf(resp.message, sizeof(resp.message), "Stopped %s", req.container_id);
        }

        write(cfd, &resp, sizeof(resp));
        close(cfd);
    }

    // shutdown
    fprintf(stderr, "Supervisor shutting down...\n");
    pthread_mutex_lock(&ctx.metadata_lock);
    for (container_record_t *c = ctx.containers; c; c = c->next)
        if (c->state == CONTAINER_RUNNING) kill(c->host_pid, SIGTERM);
    pthread_mutex_unlock(&ctx.metadata_lock);
    while (waitpid(-1, NULL, WNOHANG) > 0);
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    close(ctx.server_fd);
    unlink(CONTROL_PATH);
    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);
    return 0;
}

/* --- Client Side --- */
static int send_control_request(const control_request_t *req) {
    int fd; struct sockaddr_un addr; control_response_t res;
    if ((fd = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) return 1;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
        perror("connect (is supervisor running?)");
        close(fd); return 1;
    }
    write(fd, req, sizeof(*req));
    read(fd, &res, sizeof(res));
    printf("[%s] %s\n", res.status == 0 ? "OK" : "ERROR", res.message);
    close(fd); return res.status;
}

/* --- main --- */
int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr,
                "Usage:\n"
                "  %s supervisor <rootfs>\n"
                "  %s start <id> <rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
                "  %s run   <id> <rootfs> <command>\n"
                "  %s ps\n"
                "  %s logs  <id>\n"
                "  %s stop  <id>\n",
                argv[0], argv[0], argv[0], argv[0], argv[0], argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) return 1;
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0) {
        if (argc < 5) return 1;
        control_request_t req = {0};
        req.kind = CMD_START;
        req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
        req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
        strncpy(req.container_id, argv[2], CONTAINER_ID_LEN - 1);
        strncpy(req.rootfs, argv[3], PATH_MAX - 1);
        strncpy(req.command, argv[4], CHILD_COMMAND_LEN - 1);
        return send_control_request(&req);
    }

    if (strcmp(argv[1], "run") == 0) {
        if (argc < 5) return 1;
        control_request_t req = {0};
        req.kind = CMD_RUN;
        req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
        req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
        strncpy(req.container_id, argv[2], CONTAINER_ID_LEN - 1);
        strncpy(req.rootfs, argv[3], PATH_MAX - 1);
        strncpy(req.command, argv[4], CHILD_COMMAND_LEN - 1);
        return send_control_request(&req);
    }

    if (strcmp(argv[1], "ps") == 0) {
        control_request_t req = {0};
        req.kind = CMD_PS;
        return send_control_request(&req);
    }

    if (strcmp(argv[1], "logs") == 0) {
        if (argc < 3) return 1;
        control_request_t req = {0};
        req.kind = CMD_LOGS;
        strncpy(req.container_id, argv[2], CONTAINER_ID_LEN - 1);
        return send_control_request(&req);
    }

    if (strcmp(argv[1], "stop") == 0) {
        if (argc < 3) return 1;
        control_request_t req = {0};
        req.kind = CMD_STOP;
        strncpy(req.container_id, argv[2], CONTAINER_ID_LEN - 1);
        return send_control_request(&req);
    }

    return 1;
}
