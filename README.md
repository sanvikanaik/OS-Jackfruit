# Multi-Container Runtime

## 1. Team Information

| Name | SRN |
|------|-----|
| Sanvika | PES1UG24AM253 |
| Sanjana | PES1UG24AM249 |

---

## 2. Build, Load, and Run Instructions

### Prerequisites
```bash
sudo apt update
sudo apt install -y build-essential linux-headers-$(uname -r)
```

### Prepare Root Filesystem
```bash
mkdir -p rootfs
wget https://dl-cdn.alpinelinux.org/alpine/v3.20/releases/x86_64/alpine-minirootfs-3.20.3-x86_64.tar.gz
tar -xzf alpine-minirootfs-3.20.3-x86_64.tar.gz -C rootfs
```

### Build
```bash
make
```

### Load Kernel Module
```bash
sudo insmod monitor.ko
```

### Verify Device
```bash
ls -la /dev/container_monitor
```

### Start Supervisor (Terminal 1)
```bash
sudo ./engine supervisor ./rootfs
```

### Start Containers (Terminal 2)
```bash
# Start a container in background
sudo ./engine start alpha ./rootfs /bin/sh

# List containers
sudo ./engine ps

# View logs
sudo ./engine logs alpha

# Stop container
sudo ./engine stop alpha
```

### Run Memory Test
```bash
sudo cp memory_hog ./rootfs/
sudo ./engine start memtest ./rootfs /memory_hog
sudo dmesg | tail -20
```

### Run Scheduling Experiments
```bash
sudo cp cpu_hog ./rootfs/
sudo cp io_pulse ./rootfs/

# Different nice values
sudo ./engine start cpu1 ./rootfs /cpu_hog --nice 0
sudo ./engine start cpu2 ./rootfs /cpu_hog --nice 10

# CPU bound vs IO bound
sudo ./engine start cpu3 ./rootfs /cpu_hog --nice 0
sudo ./engine start io1 ./rootfs /io_pulse --nice 0
```

### Cleanup
```bash
sudo ./engine stop alpha
sudo rmmod monitor
sudo rm -f /tmp/mini_runtime.sock
```

---

## 3. Demo with Screenshots

### Screenshot 1 — Multi-container Supervision
> Two containers (alpha, beta) running under one supervisor process.
> <img width="372" height="97" alt="Screenshot 2026-04-17 100839" src="https://github.com/user-attachments/assets/92deb788-fd4a-4bd9-88af-9337d3332134" />


### Screenshot 2 — Metadata Tracking
> Output of `ps` command showing container ID, PID, and state.
> <img width="386" height="64" alt="Screenshot 2026-04-17 101525" src="https://github.com/user-attachments/assets/1f60ada3-3db7-45dd-8b50-8dbf06af66fc" />


### Screenshot 3 — Bounded Buffer Logging
> Log file contents captured through the logging pipeline.
> <img width="381" height="38" alt="Screenshot 2026-04-17 101623" src="https://github.com/user-attachments/assets/4f7489dc-9e64-45f7-aeca-9ad57d78676c" />


### Screenshot 4 — CLI and IPC
> CLI command being issued and supervisor responding via UNIX socket.
> <img width="371" height="215" alt="Screenshot 2026-04-17 101815" src="https://github.com/user-attachments/assets/b124b96a-3c91-4a6b-8961-5c56ba2ac0f1" />


### Screenshot 5 — Soft Limit Warning
> dmesg output showing soft limit warning for a container.
> <img width="368" height="210" alt="Screenshot 2026-04-15 074150" src="https://github.com/user-attachments/assets/801bb7cd-2365-4b34-824b-97de9a855507" />



### Screenshot 6 — Hard Limit Enforcement
> dmesg output showing container killed after exceeding hard limit.
> <img width="367" height="98" alt="Screenshot 2026-04-17 102223" src="https://github.com/user-attachments/assets/dd561a78-4ba3-4c81-b2e2-ec4374cc5062" />


### Screenshot 7 — Scheduling Experiment
> top output showing CPU usage difference between nice=0 and nice=10.
> <img width="376" height="215" alt="Screenshot 2026-04-17 102636" src="https://github.com/user-attachments/assets/68c1a52c-fa7e-46fe-9fd4-bbe754d619d1" />


### Screenshot 8 — Clean Teardown
> Evidence of clean shutdown with no zombie processes.
> <img width="375" height="158" alt="Screenshot 2026-04-17 103058" src="https://github.com/user-attachments/assets/619b5607-a57a-4100-97c0-164d7f4de198" />


---

## 4. Engineering Analysis

### 1. Isolation Mechanisms
The runtime achieves isolation using Linux namespaces. Each container gets its own PID namespace (so container processes cannot see host processes), UTS namespace (so each container has its own hostname), and mount namespace (so filesystem changes don't affect the host). `chroot` restricts the container's view of the filesystem to the Alpine rootfs. The host kernel is still shared — containers share the same kernel, network stack, and kernel resources. This is lighter than full virtualization but means a kernel exploit inside a container can affect the host.

### 2. Supervisor and Process Lifecycle
A long-running supervisor is useful because it maintains state about all containers, handles their output, and reaps them when they exit. Without a supervisor, child processes become zombies when they exit because no parent calls `waitpid`. The supervisor installs a `SIGCHLD` handler that calls `waitpid` with `WNOHANG` to reap all exited children immediately. Container metadata is tracked in a linked list protected by a mutex so concurrent CLI requests don't corrupt state.

### 3. IPC, Threads, and Synchronization
The project uses two IPC mechanisms. A pipe carries container output to the supervisor (producer-consumer logging). A UNIX domain socket carries CLI commands to the supervisor. The bounded buffer uses a mutex to protect the shared buffer, and two condition variables (`not_empty`, `not_full`) to block producers when full and consumers when empty. Without synchronization, producers and consumers could corrupt the buffer by reading and writing simultaneously, or miss wake-up signals leading to deadlock.

### 4. Memory Management and Enforcement
RSS (Resident Set Size) measures the actual physical RAM a process is currently using. It does not measure memory that has been allocated but not yet touched, or memory that has been swapped out. Soft and hard limits serve different purposes — a soft limit is a warning threshold that alerts the operator without disrupting the container, while a hard limit is a hard enforcement point that terminates the process. Enforcement belongs in kernel space because the kernel has direct access to process memory maps via `task_struct` and `mm_struct`, and can act immediately without relying on the process itself to cooperate.

### 5. Scheduling Behavior
Linux uses the Completely Fair Scheduler (CFS) which assigns CPU time based on priority. The `nice` value adjusts a process's weight — lower nice means higher priority and more CPU time. In our experiments, `cpu1` (nice=0) consistently received more CPU time than `cpu2` (nice=10), demonstrating that CFS respects priority weights. For CPU-bound vs IO-bound workloads, the CPU-bound process dominated CPU usage while the IO-bound process spent most of its time in a waiting state, showing that CFS naturally gives more CPU to runnable processes.

---

## 5. Design Decisions and Tradeoffs

### Namespace Isolation
**Choice:** Used `CLONE_NEWPID`, `CLONE_NEWUTS`, `CLONE_NEWNS` with `clone()`.
**Tradeoff:** No network namespace isolation, so containers share the host network.
**Justification:** Sufficient for the scope of this project and avoids complexity of network setup.

### Supervisor Architecture
**Choice:** Single long-running supervisor with a UNIX socket for CLI communication.
**Tradeoff:** If the supervisor crashes, all container metadata is lost.
**Justification:** Simple and reliable for managing multiple containers with a single control point.

### IPC and Logging
**Choice:** Pipes for container output, UNIX socket for CLI commands.
**Tradeoff:** The bounded buffer adds latency compared to direct file writes.
**Justification:** Decouples container output from disk I/O, preventing slow disk writes from blocking containers.

### Kernel Monitor
**Choice:** Periodic timer checking RSS every 5 seconds.
**Tradeoff:** A container could exceed its hard limit for up to 5 seconds before being killed.
**Justification:** Polling every 5 seconds is a good balance between responsiveness and kernel overhead.

### Scheduling Experiments
**Choice:** Used `nice` values to demonstrate scheduling differences.
**Tradeoff:** `nice` only affects CPU scheduling, not I/O or memory priority.
**Justification:** Simple to demonstrate and directly observable using `top`.

---

## 6. Scheduler Experiment Results

### Experiment 1 — Different Nice Values

| Container | Nice Value | CPU % Observed |
|-----------|-----------|----------------|
| cpu1 | 0 | ~65% |
| cpu2 | 10 | ~35% |

**Observation:** cpu1 with nice=0 received significantly more CPU time than cpu2 with nice=10. This demonstrates that CFS allocates CPU proportionally based on process weight.

### Experiment 2 — CPU Bound vs IO Bound

| Container | Type | CPU % Observed |
|-----------|------|----------------|
| cpu3 | CPU bound | ~95% |
| io1 | IO bound | ~5% |

**Observation:** The CPU-bound process dominated CPU usage while the IO-bound process spent most of its time waiting for I/O completion. CFS naturally schedules runnable processes over waiting ones, giving the CPU-bound workload almost all available CPU time.
