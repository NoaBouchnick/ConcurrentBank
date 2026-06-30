# 🏦 ConcurrentBank: Multi-Process Banking System

A concurrent banking system based on multiple processes and threads, implementing advanced Operating Systems concepts in C.

## 📋 Project Description
The system simulates various bank branches, where each branch is a separate process executing transactions using multiple threads. The project demonstrates a deep understanding of:

* **Thread Synchronization:** Using Mutexes to prevent Race Conditions when accessing accounts.
* **Process Synchronization (Bonus A):** Using a Semaphore to limit the execution load (up to 2 branches running concurrently).
* **Inter-Process Communication (IPC):** Transferring aggregated data from the child branches to the parent process via Pipes.
* **Signal Handling:** Managing signals (`SIGINT`, `SIGTERM`, `SIGCHLD`) for graceful shutdown and zombie process prevention.
* **Memory Management:** Utilizing the OS's Copy-on-Write (COW) mechanism after `fork`, and `mmap` for shared memory.

---

## 🚀 Run Instructions
*Requires Docker Desktop and Docker Compose installed on your system.*

### 1. Build and Run (Safe Mode)
To run the system with Mutex protection and full synchronization:
```bash
docker compose build
docker compose up
```

### 2. Race Condition Demonstration (No Locks)
To run the system under heavy load without Mutex protection, demonstrating memory collisions and race conditions:
```bash
docker compose run -e N_TX=100000 bank make run_race
```

### 3. View Logs
After a successful run, you can view the transaction log generated in the logs directory:
```bash
cat logs/transactions.log
```

### 📂 Project Structure
* bank.c: Main C source code of the bank.

* Makefile: Compilation definitions (including support for NO_LOCK mode and easy run targets).

* Dockerfile: Container environment build instructions.

* docker-compose.yml: Container orchestration and environment variable injection.

* accounts.txt: Initial account database loaded at startup.

* answers.pdf: Theoretical question answers and experiment screenshots.

* logs/: Output directory (mapped to the host) for transaction logs.

This project was built for Assignment 3 in the Operating Systems course.