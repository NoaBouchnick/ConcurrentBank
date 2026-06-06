#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/wait.h>
#include <signal.h>
#include <string.h>
#include <time.h>
#include <semaphore.h>
#include <sys/mmap.h>

#define MAX_ACCOUNTS 64
#define DEFAULT_N_BRANCHES 3
#define DEFAULT_M_THREADS 5
#define DEFAULT_N_TX 20
#define MAX_ACTIVE 2 // BONUS A

typedef struct {
    int id;
    char owner[64];
    long balance;
    int tx_count;
    pthread_mutex_t lock;
} account_t;

account_t accounts[MAX_ACCOUNTS];
int num_accounts = 0;
long initial_total_balance = 0;

// BONUS A: Pointer to shared semaphore
sem_t *branch_sem;

typedef struct {
    int branch_id;
    int thread_id;
    int n_tx;
} thread_args_t;

typedef struct {
    int branch_id;
    int total_transactions;
    long total_deposited;
    long total_withdrawn;
} branch_result_t;

volatile sig_atomic_t keep_running = 1;

// Signal handler for SIGINT, SIGTERM, and SIGCHLD
void handle_signal(int sig) {
    if (sig == SIGINT || sig == SIGTERM) {
        keep_running = 0;
        const char *msg = "\nReceived signal - waiting for branches to finish...\n";
        write(STDOUT_FILENO, msg, strlen(msg));
    }
    else if (sig == SIGCHLD) {
        // Zombie prevention
    }
}

void load_accounts(const char *filename) {
    int fd = open(filename, O_RDONLY);
    if (fd < 0) {
        perror("Failed to open accounts file");
        exit(1);
    }

    char buffer[4096];
    ssize_t bytes_read = read(fd, buffer, sizeof(buffer) - 1);
    if (bytes_read < 0) {
        perror("Failed to read accounts");
        close(fd);
        exit(1);
    }
    buffer[bytes_read] = '\0';
    close(fd);

    printf("Loading accounts:\n");
    printf("ID\tOwner\t\tBalance\n");
    printf("-----------------------------------\n");

    char *line = strtok(buffer, "\n");
    while (line != NULL && num_accounts < MAX_ACCOUNTS) {
        if (line[0] != '#' && strlen(line) > 0) {
            sscanf(line, "%d %63s %ld", &accounts[num_accounts].id, 
                   accounts[num_accounts].owner, &accounts[num_accounts].balance);
            accounts[num_accounts].tx_count = 0;
            initial_total_balance += accounts[num_accounts].balance;
            pthread_mutex_init(&accounts[num_accounts].lock, NULL);
            
            printf("%d\t%-10s\t$%ld\n", accounts[num_accounts].id, accounts[num_accounts].owner, accounts[num_accounts].balance);
            num_accounts++;
        }
        line = strtok(NULL, "\n");
    }
}

void *worker_thread(void *arg) {
    thread_args_t *args = (thread_args_t *)arg;
    unsigned int seed = time(NULL) ^ (args->branch_id << 16) ^ args->thread_id;
    
    int log_fd = open("logs/transactions.log", O_WRONLY | O_CREAT | O_APPEND, 0644);
    
    long thread_deposited = 0;
    long thread_withdrawn = 0;

    for (int i = 0; i < args->n_tx && keep_running; i++) {
        int acc_idx = rand_r(&seed) % num_accounts;
        int amount = (rand_r(&seed) % 500) + 1;
        int is_deposit = rand_r(&seed) % 2;

#ifndef NO_LOCK
        pthread_mutex_lock(&accounts[acc_idx].lock);
#endif

        if (is_deposit) {
            accounts[acc_idx].balance += amount;
            thread_deposited += amount;
        } else {
            if (accounts[acc_idx].balance >= amount) {
                accounts[acc_idx].balance -= amount;
                thread_withdrawn += amount;
            } else {
                is_deposit = -1; // Skip
            }
        }
        
        if (is_deposit != -1) {
            accounts[acc_idx].tx_count++;
        }

#ifndef NO_LOCK
        pthread_mutex_unlock(&accounts[acc_idx].lock);
#endif

        if (is_deposit != -1 && log_fd >= 0) {
            char log_entry[256];
            snprintf(log_entry, sizeof(log_entry), 
                "Time: %ld | Branch: %d | Thread: %d | Acc: %d | %s | Amount: %d | New Balance: %ld\n",
                time(NULL), args->branch_id, args->thread_id, accounts[acc_idx].id,
                is_deposit ? "DEPOSIT" : "WITHDRAW", amount, accounts[acc_idx].balance);
            write(log_fd, log_entry, strlen(log_entry));
        }
    }
    
    if (log_fd >= 0) close(log_fd);
    
    branch_result_t *res = malloc(sizeof(branch_result_t));
    res->total_transactions = args->n_tx;
    res->total_deposited = thread_deposited;
    res->total_withdrawn = thread_withdrawn;
    pthread_exit((void*)res);
}

void run_branch(int branch_id, int m_threads, int n_tx, int write_pipe) {
    pthread_t threads[m_threads];
    thread_args_t args[m_threads];
    branch_result_t total_res = {branch_id, 0, 0, 0};

    // ============================================
    // BONUS A: Counting Semaphore Throttle
    // Why this prevents overloading the server:
    // By using a shared counting semaphore initialized to MAX_ACTIVE (2),
    // we guarantee that no matter how many branches are spawned, only a fixed
    // number of them can execute their CPU-intensive worker threads simultaneously.
    // This bounds the maximum number of active threads, thereby reducing context-switching
    // overhead, memory consumption, and extreme contention on the shared account mutexes.
    // ============================================
    sem_wait(branch_sem); // Wait for an available slot

    for (int i = 0; i < m_threads; i++) {
        args[i].branch_id = branch_id;
        args[i].thread_id = i;
        args[i].n_tx = n_tx;
        pthread_create(&threads[i], NULL, worker_thread, &args[i]);
    }

    for (int i = 0; i < m_threads; i++) {
        void *thread_res;
        pthread_join(threads[i], &thread_res);
        branch_result_t *res = (branch_result_t*)thread_res;
        total_res.total_transactions += res->total_transactions;
        total_res.total_deposited += res->total_deposited;
        total_res.total_withdrawn += res->total_withdrawn;
        free(res);
    }

    sem_post(branch_sem); // Release the slot for the next branch
    // ============================================

    // === RACE CONDITION DEMO CHECK ===
    long branch_final_balance = 0;
    for (int i = 0; i < num_accounts; i++) {
        branch_final_balance += accounts[i].balance;
    }
    long expected_balance = initial_total_balance + total_res.total_deposited - total_res.total_withdrawn;
    
    if (branch_final_balance != expected_balance) {
        printf(">>> Branch %d Race Condition Check: FAILED (Expected %ld, Got %ld)\n", 
               branch_id, expected_balance, branch_final_balance);
    } else {
        printf(">>> Branch %d Race Condition Check: PASSED\n", branch_id);
    }
    // =================================

    write(write_pipe, &total_res, sizeof(branch_result_t));
    close(write_pipe);
    exit(0);
}

int main() {
    struct sigaction sa;
    sa.sa_handler = handle_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGCHLD, &sa, NULL);

    load_accounts("accounts.txt");

    int n_branches = getenv("N_BRANCHES") ? atoi(getenv("N_BRANCHES")) : DEFAULT_N_BRANCHES;
    int m_threads = getenv("M_THREADS") ? atoi(getenv("M_THREADS")) : DEFAULT_M_THREADS;
    int n_tx = getenv("N_TX") ? atoi(getenv("N_TX")) : DEFAULT_N_TX;

    // BONUS A: Allocate semaphore in shared memory so child processes can share it
    branch_sem = mmap(NULL, sizeof(sem_t), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (branch_sem == MAP_FAILED) {
        perror("mmap failed");
        exit(1);
    }
    sem_init(branch_sem, 1, MAX_ACTIVE); // pshared = 1, value = MAX_ACTIVE

    int pipes[n_branches][2];
    pid_t children[n_branches];

    for (int i = 0; i < n_branches; i++) {
        if (pipe(pipes[i]) < 0) {
            perror("Pipe failed");
            exit(1);
        }
        
        pid_t pid = fork();
        if (pid == 0) { 
            close(pipes[i][0]); 
            run_branch(i, m_threads, n_tx, pipes[i][1]);
        } else if (pid > 0) { 
            children[i] = pid;
            close(pipes[i][1]); 
        } else {
            perror("Fork failed");
            exit(1);
        }
    }

    // Wait for all children to prevent zombies
    int status;
    for (int i = 0; i < n_branches; i++) {
        waitpid(children[i], &status, 0);
        if (WIFEXITED(status) && WEXITSTATUS(status) != 0) {
            printf("Warning: Child %d exited with non-zero status.\n", children[i]);
        }
    }

    printf("\n=== Final Bank Report ===\n");
    long grand_total_deposited = 0;
    long grand_total_withdrawn = 0;
    int grand_total_tx = 0;

    for (int i = 0; i < n_branches; i++) {
        branch_result_t res;
        read(pipes[i][0], &res, sizeof(branch_result_t));
        printf("Branch %d: %d transactions | +%ld deposited | -%ld withdrawn\n", 
               res.branch_id, res.total_transactions, res.total_deposited, res.total_withdrawn);
        grand_total_tx += res.total_transactions;
        grand_total_deposited += res.total_deposited;
        grand_total_withdrawn += res.total_withdrawn;
        close(pipes[i][0]);
    }
    
    printf("Total: %d transactions | +%ld deposited | -%ld withdrawn\n", 
           grand_total_tx, grand_total_deposited, grand_total_withdrawn);

    printf("\nAccount balances after all transactions:\n");
    long final_total_balance = 0;
    for (int i = 0; i < num_accounts; i++) {
        printf("%d %s: $%ld\n", accounts[i].id, accounts[i].owner, accounts[i].balance);
        final_total_balance += accounts[i].balance;
        pthread_mutex_destroy(&accounts[i].lock);
    }

    printf("\nBalance conservation check: %s (initial sum %ld == final sum %ld)\n",
           (initial_total_balance == final_total_balance) ? "PASSED" : "FAILED",
           initial_total_balance, final_total_balance);

    // BONUS A: Cleanup semaphore
    sem_destroy(branch_sem);
    munmap(branch_sem, sizeof(sem_t));

    return 0;
}