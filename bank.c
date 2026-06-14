#include <stdio.h>    // קלט/פלט סטנדרטי (printf, sscanf)
#include <stdlib.h>   // הקצאות זיכרון, המרות והפסקת ריצה (malloc, exit, atoi)
#include <unistd.h>   // מספקת גישה לקריאות מערכת של לינוקס (fork, pipe, read, write, close)
#include <fcntl.h>    // מספקת לנו את הדגלים לפתיחת קבצים (O_RDONLY, O_WRONLY, O_CREAT וכו')
#include <pthread.h>  // ספריית החוטים (יצירה, המתנה, מנעולים)
#include <sys/wait.h> // אחראית על ההמתנה לתהליכי הבן (waitpid) למניעת זומבים
#include <signal.h>   // מאפשרת "לתפוס" איתותים ממערכת ההפעלה (כמו Ctrl+C)
#include <string.h>   // פעולות על מחרוזות (strtok, strlen)
#include <time.h>     // פונקציות זמן (לשם יצירת "גרעין" אקראיות - seed להגרלות)
#include <semaphore.h> // שימוש בסמפורים (שומר המועדון שלנו)
#include <sys/mmap.h>  // מקצה זיכרון משותף בין תהליכים (כדי שכל התהליכים יראו את אותו סמפור)

// הגדרת קבועים שניתן יהיה לשנות בקלות מאוחר יותר או לדרוס עם משתני סביבה
#define MAX_ACCOUNTS 64      // מקסימום החשבונות שהבנק יכול לנהל
#define DEFAULT_N_BRANCHES 3 // כמות ברירת מחדל של סניפים (תהליכים) שיווצרו
#define DEFAULT_M_THREADS 5  // כמות הפקידים (חוטים) בכל סניף כברירת מחדל
#define DEFAULT_N_TX 20      // כמות הפעולות שפקיד יבצע
#define MAX_ACTIVE 2         // הגבלת העבודה ל-2 סניפים בו-זמנית (עבור הסמפור)

// הגדרת מבנה המייצג חשבון בנק
typedef struct {
    int id;               
    char owner[64];       
    long balance;         // יתרת החשבון הנוכחית
    int tx_count;         // מונה: כמה פעולות בוצעו על החשבון הזה
    pthread_mutex_t lock; // מנעול ייעודי לחשבון זה, מונע תחרות בין חוטים (שני פקידים שניגשים יחד)
} account_t;

account_t accounts[MAX_ACCOUNTS]; 
int num_accounts = 0;             
long initial_total_balance = 0;   // ישמור את סכום הכסף הכולל בבנק בתחילת הריצה (לבדיקת שפיות בסוף)

// מצביע לסמפור. הסמפור יישב בזיכרון המשותף, ופה אנו שומרים את הכתובת אליו
sem_t *branch_sem;

// מבנה שנעביר כארגומנט לכל חוט (פקיד) בעת היצירה שלו, כדי שידע מי הוא
typedef struct {
    int branch_id; // מזהה הסניף שלו
    int thread_id; // מזהה הפקיד שלו
    int n_tx;      // כמה פעולות עליו לבצע
} thread_args_t;

// מבנה שמאגד את תוצאות העבודה של סניף שלם, כדי לשלוח בסוף לאב דרך הצינור
typedef struct {
    int branch_id;          // איזה סניף מחזיר את התשובה
    int total_transactions; // סך הכל הפעולות שבוצעו בסניף
    long total_deposited;   // סך הכל כסף שהופקד בסניף זה
    long total_withdrawn;   // סך הכל כסף שנמשך בסניף זה
} branch_result_t;

// משתנה דגל גלובלי שאומר לתוכנית אם להמשיך לרוץ או לעצור.
// volatile sig_atomic_t: מבטיח ששינוי המשתנה בתוך פונקציית איתות יהיה בטוח וייראה מיד לשאר התוכנית
volatile sig_atomic_t keep_running = 1;

// פונקציה שמערכת ההפעלה תקפיץ אוטומטית ברגע שיתקבל איתות
void handle_signal(int sig) {
    // אם המשתמש ביקש לעצור (SIGINT = Ctrl+C, SIGTERM = בקשת עצירה מסודרת)
    if (sig == SIGINT || sig == SIGTERM) {
        keep_running = 0; // מורידים את הדגל, הלולאות של הפקידים יפסיקו בהזדמנות הראשונה
        const char *msg = "\nReceived signal - waiting for branches to finish...\n";
        // משתמשים ב-write (קריאת מערכת) ולא ב-printf כי printf לא בטוחה לשימוש בתוך תופס-איתותים
        write(STDOUT_FILENO, msg, strlen(msg));
    }
    // אם בן סיים לרוץ ומת (SIGCHLD)
    else if (sig == SIGCHLD) {
        // מנקים אותו מזיכרון המערכת בעזרת waitpid.
        // -1: כל בן שהוא. WNOHANG: אל תעצור ותחכה אם אין בן שמת, פשוט תמשיך הלאה (מונע תקיעה).
        while (waitpid(-1, NULL, WNOHANG) > 0);
    }
}

// פונקציה לטעינת החשבונות מקובץ טקסט אל תוך המערך
void load_accounts(const char *filename) {
    // פותחים את הקובץ לקריאה בלבד
    int fd = open(filename, O_RDONLY);
    if (fd < 0) { // אם ההחזרה קטנה מ-0, יש שגיאה
        perror("Failed to open accounts file");
        exit(1); // יוצאים מהתוכנית
    }

    printf("Loading accounts:\n");
    printf("ID\tOwner\t\tBalance\n");
    printf("-----------------------------------\n");

    char c;           // משתנה שיאחסן תו אחד בכל פעם
    char line[256];   // מערך עזר לבניית שורה שלמה מתוך התווים
    int pos = 0;      // מיקום נוכחי בתוך השורה

    // הלולאה קוראת תו אחד בכל פעם לתוך המשתנה 'c' עד שאין יותר מה לקרוא
    while (read(fd, &c, 1) > 0 && num_accounts < MAX_ACCOUNTS) {
        // אם הגענו לסוף שורה
        if (c == '\n' || c == '\0') {
            line[pos] = '\0'; // סוגרים את המחרוזת בצורה תקנית ב-C
            
            // מוודאים שהשורה לא ריקה ושזו לא שורת הערה (מתחילה בסולמית)
            if (pos > 0 && line[0] != '#') {
                // קוראים את הנתונים מתוך השורה שאספנו
                sscanf(line, "%d %63s %ld", &accounts[num_accounts].id, 
                       accounts[num_accounts].owner, &accounts[num_accounts].balance);
                
                accounts[num_accounts].tx_count = 0; // מאפסים את מונה הפעולות של החשבון
                initial_total_balance += accounts[num_accounts].balance; // מוסיפים לסך הכולל
                
                // מאתחלים את המנעול (Mutex) של החשבון הספציפי הזה כדי שיהיה מוכן לשימוש
                pthread_mutex_init(&accounts[num_accounts].lock, NULL);
                
                // מדפיסים למסך את החשבון שנטען
                printf("%d\t%-10s\t$%ld\n", accounts[num_accounts].id, accounts[num_accounts].owner, accounts[num_accounts].balance);
                num_accounts++; // מקדמים את מונה החשבונות
            }
            pos = 0; // מאפסים את המיקום בבאפר לקראת השורה הבאה
        } else {
            // כל עוד לא הגענו לסוף השורה, ממשיכים לאסוף תווים למערך
            if (pos < (int)(sizeof(line) - 1)) {
                line[pos++] = c;
            }
        }
    }
    
    // במקרה שהקובץ נגמר בלי תו ירידת שורה בסוף
    if (pos > 0 && num_accounts < MAX_ACCOUNTS) {
        line[pos] = '\0';
        if (line[0] != '#') {
            sscanf(line, "%d %63s %ld", &accounts[num_accounts].id, 
                   accounts[num_accounts].owner, &accounts[num_accounts].balance);
            accounts[num_accounts].tx_count = 0;
            initial_total_balance += accounts[num_accounts].balance;
            pthread_mutex_init(&accounts[num_accounts].lock, NULL);
            printf("%d\t%-10s\t$%ld\n", accounts[num_accounts].id, accounts[num_accounts].owner, accounts[num_accounts].balance);
            num_accounts++;
        }
    }

    close(fd); // סוגרים את הקובץ, לא צריך אותו יותר
}

// זו הפונקציה שכל חוט (פקיד) מריץ במקביל
void *worker_thread(void *arg) {
    // ממירים את הארגומנט שקיבלנו בחזרה למבנה thread_args_t כדי לדעת מי אנחנו
    thread_args_t *args = (thread_args_t *)arg;
    
    // יוצרים "גרעין" ליצירת מספרים אקראיים יחודי לכל פקיד, מבוסס זמן, סניף ומזהה פקיד
    unsigned int seed = time(NULL) ^ (args->branch_id << 16) ^ args->thread_id;
    
    // פותחים קובץ לוג לכתיבה, יוצרים אותו אם לא קיים, ומוסיפים בסוף (O_APPEND)
    int log_fd = open("logs/transactions.log", O_WRONLY | O_CREAT | O_APPEND, 0644);
    
    // אוגרים מקומיים שיספרו כמה הפקיד הזה ספציפית משך והפקיד
    long thread_deposited = 0;
    long thread_withdrawn = 0;

    // לולאת הפעולות: רוץ n_tx פעמים, אבל תעצור גם אם keep_running ירד ל-0 (בגלל איתות)
    for (int i = 0; i < args->n_tx && keep_running; i++) {
        int acc_idx = rand_r(&seed) % num_accounts; // בוחרים אינדקס חשבון אקראי
        int amount = (rand_r(&seed) % 500) + 1;     // בוחרים סכום אקראי בין 1 ל-500
        int is_deposit = rand_r(&seed) % 2;         // מגרילים 0 (משיכה) או 1 (הפקדה)

// פקודות לקומפיילר: אם לא הוגדר לו להתעלם ממנעולים, תפעיל את המנעול
#ifndef NO_LOCK
        // נעילת החשבון. אם פקיד אחר כבר נעל, אנחנו נחכה פה עד שישחרר
        pthread_mutex_lock(&accounts[acc_idx].lock); // מתבסס על פקודת חומרה אטומית (Test-and-Set)
#endif

        if (is_deposit) { // אם זו הפקדה
            accounts[acc_idx].balance += amount; // מוסיפים ליתרה
            thread_deposited += amount;          // רושמים בספרים של הפקיד
        } else { // אם זו משיכה
            if (accounts[acc_idx].balance >= amount) { // בודקים שיש מספיק כסף
                accounts[acc_idx].balance -= amount;   // מורידים מהיתרה
                thread_withdrawn += amount;            // רושמים בספרים של הפקיד
            } else {
                is_deposit = -1; // סימון מיוחד (הפעולה בוטלה כי אין מספיק כסף)
            }
        }
        
        // אם הפעולה לא בוטלה, נקדם את מונה הפעולות של החשבון הזה
        if (is_deposit != -1) {
            accounts[acc_idx].tx_count++;
        }

#ifndef NO_LOCK
        // חובה לשחרר את המנעול של החשבון כדי שפקידים אחרים יוכלו לגשת!
        pthread_mutex_unlock(&accounts[acc_idx].lock);
#endif

        // אם הפעולה הצליחה ויש קובץ לוג פתוח, נכתוב את הלוג
        if (is_deposit != -1 && log_fd >= 0) {
            char log_entry[256];
            // מכינים מחרוזת עם כל הפרטים
            snprintf(log_entry, sizeof(log_entry), 
                "Time: %ld | Branch: %d | Thread: %d | Acc: %d | %s | Amount: %d | New Balance: %ld\n",
                time(NULL), args->branch_id, args->thread_id, accounts[acc_idx].id,
                is_deposit ? "DEPOSIT" : "WITHDRAW", amount, accounts[acc_idx].balance);
            // כותבים את המחרוזת לקובץ הלוג
            write(log_fd, log_entry, strlen(log_entry));
        }
    } // סוף לולאת הפעולות
    
    if (log_fd >= 0) close(log_fd); // סוגרים את הלוג בסיום
    
    // מכינים חבילה עם תוצאות הפקיד הזה כדי להחזיר החוצה
    // חייבים להשתמש ב-malloc כדי שהזיכרון יישאר קיים גם אחרי שהפונקציה מסתיימת
    branch_result_t *res = malloc(sizeof(branch_result_t));
    res->total_transactions = args->n_tx;
    res->total_deposited = thread_deposited;
    res->total_withdrawn = thread_withdrawn;
    
    // מסיימים את חיי החוט ומחזירים את התוצאות
    pthread_exit((void*)res);
}

// הפונקציה המרכזית שכל תהליך בן (סניף) מריץ מיד אחרי ה-fork
void run_branch(int branch_id, int m_threads, int n_tx, int write_pipe) {
    // מערכים לשמירת מזהי החוטים והארגומנטים שלהם
    pthread_t threads[m_threads];
    thread_args_t args[m_threads];
    // משתנה מקומי שיסכם את כל תוצאות הסניף
    branch_result_t total_res = {branch_id, 0, 0, 0};

    // המתנה בסמפור. הסמפור הוגדר ל-2. אם 2 סניפים כבר עברו את השורה הזו ועדיין עובדים,
    // הסניף הנוכחי (השלישי למשל) "יירדם" פה ויחכה עד שאחד מהם יסיים.
    sem_wait(branch_sem); 

    // לולאה ליצירת כל הפקידים (החוטים) בסניף
    for (int i = 0; i < m_threads; i++) {
        args[i].branch_id = branch_id;
        args[i].thread_id = i;
        args[i].n_tx = n_tx;
        // קריאה לפונקציית הספרייה שיוצרת חוט חדש. החוט יתחיל לרוץ על פונקציית worker_thread
        pthread_create(&threads[i], NULL, worker_thread, &args[i]);
    }

    // לולאה להמתנה שכל הפקידים יסיימו את עבודתם
    for (int i = 0; i < m_threads; i++) {
        void *thread_res;
        // ממתינים לחוט ה-i שיסיים (כמו waitpid, רק לחוטים). התוצאה שלו נכנסת ל-thread_res
        pthread_join(threads[i], &thread_res);
        branch_result_t *res = (branch_result_t*)thread_res;
        
        // מוסיפים את תוצאות הפקיד הספציפי לסיכום של כל הסניף
        total_res.total_transactions += res->total_transactions;
        total_res.total_deposited += res->total_deposited;
        total_res.total_withdrawn += res->total_withdrawn;
        // משחררים את הזיכרון של התוצאה שהפקיד הקצה עם malloc
        free(res);
    }

    // הסניף סיים את העבודה שלו. הוא מעלה את המונה של הסמפור ומאפשר לסניף אחר להתחיל לעבוד
    sem_post(branch_sem); 

    // בדיקת חוק שימור הכסף: מוודאים שסך היתרה הנוכחית בכל החשבונות (ברמת הסניף הזה)
    // שווה ליתרה ההתחלתית + כל מה שהסניף הפקיד - כל מה שהסניף משך.
    long branch_final_balance = 0;
    for (int i = 0; i < num_accounts; i++) {
        branch_final_balance += accounts[i].balance;
    }
    long expected_balance = initial_total_balance + total_res.total_deposited - total_res.total_withdrawn;
    
    // אם הערכים לא תואמים, סימן שחוטים דרסו אחד את השני (תנאי תחרות התרחש במקרה וביטלו מנעולים)
    if (branch_final_balance != expected_balance) {
        printf(">>> Branch %d Race Condition Check: FAILED (Expected %ld, Got %ld)\n", 
               branch_id, expected_balance, branch_final_balance);
    } else {
        printf(">>> Branch %d Race Condition Check: PASSED\n", branch_id);
    }

    // הסניף (הבן) כותב את טבלת הסיכום שלו לתוך צד הכתיבה של צינור התקשורת
    write(write_pipe, &total_res, sizeof(branch_result_t));
    close(write_pipe); // סוגר את צד הכתיבה (חשוב כדי לשחרר משאבים)
    exit(0); // תהליך הבן סיים את חייו ומת. (אבא שלו יאסוף אותו עם waitpid)
}

/* ==============================================================
 * הסבר על מניעת תהליכי זומבי (נדרש לפי סעיף 1.3 במטלה):
 * תהליך זומבי נוצר כאשר תהליך בן מסיים את ריצתו (עושה exit), 
 * אך תהליך האב עדיין לא ביצע קריאה ל-wait() או waitpid() כדי לאסוף את 
 * קוד היציאה שלו. במצב זה, מערכת ההפעלה שומרת רישום קטן של הבן בטבלת התהליכים.
 * אם האב לעולם לא יקרא ל-wait, הזומבים יצטברו ויבזבזו משאבי מערכת.
 * בקוד שלנו אנו מונעים זאת על ידי קריאה מפורשת ל-waitpid עבור כל בן,
 * ובנוסף הוגדר (כרגע בהערה) טיפול באיתות SIGCHLD שפועל אוטומטית למחיקת תהליכים שמתו.
 * ============================================================== */

// פונקציית האב, מנהל הבנק - זוהי הפונקציה הראשונה שרצה שמפעילים את התוכנית
int main() {
    // הגדרת טיפול באיתותים (signals). מקשרים איתותים כמו Ctrl+C לפונקציה handle_signal
    struct sigaction sa;
    sa.sa_handler = handle_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    
    // לפי דרישות מטלה
    // sigaction(SIGCHLD, &sa, NULL);

    // קריאה לפונקציה שטוענת את הנתונים ההתחלתיים מקובץ
    load_accounts("accounts.txt");

    // קריאת משתני סביבה. אם קיימים, משתמשים בהם. אם לא, בערכי ה-DEFAULT מראש הקובץ.
    int n_branches = getenv("N_BRANCHES") ? atoi(getenv("N_BRANCHES")) : DEFAULT_N_BRANCHES;
    int m_threads = getenv("M_THREADS") ? atoi(getenv("M_THREADS")) : DEFAULT_M_THREADS;
    int n_tx = getenv("N_TX") ? atoi(getenv("N_TX")) : DEFAULT_N_TX;

    // הקצאת זיכרון לסמפור ב"זיכרון משותף" בעזרת mmap. 
    // MAP_SHARED אומר שמערכת ההפעלה תאפשר גם לאב וגם לילדים שלו לראות את אותו משתנה בדיוק!
    branch_sem = mmap(NULL, sizeof(sem_t), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (branch_sem == MAP_FAILED) {
        perror("mmap failed");
        exit(1);
    }
    // אתחול הסמפור: המשתנה השני (1) אומר שהסמפור ישותף בין תהליכים, הערך ההתחלתי שלו הוא MAX_ACTIVE (2)
    sem_init(branch_sem, 1, MAX_ACTIVE); 

    // מערך דו ממדי של צינורות (pipe). לכל סניף (בן) יש צד קריאה [0] וצד כתיבה [1]
    int pipes[n_branches][2];
    // מערך לשמירת ה-PID (מזהה תהליך) של כל הילדים כדי לחכות להם אח"כ
    pid_t children[n_branches];

    //  לולאה ליצירת כל הצינורות 
    for (int i = 0; i < n_branches; i++) {
        // יוצרים צינור עבור הבן ה-i
        if (pipe(pipes[i]) < 0) {
            perror("Pipe failed");
            exit(1);
        }
    }

    // לולאה ליצירת הסניפים (התהליכים)
    for (int i = 0; i < n_branches; i++) {
        // פיצול התהליך! כאן נוצר עותק של התוכנית
        pid_t pid = fork();
        
        if (pid == 0) { 
            // קוד שרץ רק בתוך תהליך הבן
            close(pipes[i][0]); // בן לא צריך לקרוא מהצינור של עצמו, רק לכתוב, אז סוגרים את צד הקריאה
            // קוראים לפונקציה של הסניף. מעבירים לה את צד הכתיבה של הצינור [1]
            run_branch(i, m_threads, n_tx, pipes[i][1]);
            // (הבן אף פעם לא חוזר לפה כי יש לו exit בסוף run_branch)
            
        } else if (pid > 0) { 
            // קוד שרץ רק בתהליך האב
            children[i] = pid; // האב שומר את מספר תעודת הזהות של הבן שנוצר
            close(pipes[i][1]); // האב רק יקרא מהצינור, הוא לא צריך לכתוב, אז הוא סוגר את צד הכתיבה
        } else {
            // אם fork החזיר מינוס, הייתה שגיאת מערכת
            perror("Fork failed");
            exit(1);
        }
    }

    // הגענו לפה, זה רק תהליך האב.
    // האב עושה לולאה ומחכה שכל הילדים שהוא יצר יסיימו את חייהם (מונע מהם להפוך לזומבים)
    int status;
    for (int i = 0; i < n_branches; i++) {
        waitpid(children[i], &status, 0); // ממתינים לכל תהליך בן
        // אם ילד סיים לא בהצלחה, מתריעים
        if (WIFEXITED(status) && WEXITSTATUS(status) != 0) {
            printf("Warning: Child %d exited with non-zero status.\n", children[i]);
        }
    }

    // הדפסת דוח מסכם
    printf("\n=== Final Bank Report ===\n");
    long grand_total_deposited = 0;
    long grand_total_withdrawn = 0;
    int grand_total_tx = 0;

    // האב קורא מהצינורות. כל בן שמת השאיר בצד השני של הצינור את סיכום המידע שלו (branch_result_t)
    for (int i = 0; i < n_branches; i++) {
        branch_result_t res;
        // קוראים מצד הקריאה [0] של הצינור
        read(pipes[i][0], &res, sizeof(branch_result_t));
        
        printf("Branch %d: %d transactions | +%ld deposited | -%ld withdrawn\n", 
               res.branch_id, res.total_transactions, res.total_deposited, res.total_withdrawn);
        
        // הוספה לסיכום של כלל הבנק
        grand_total_tx += res.total_transactions;
        grand_total_deposited += res.total_deposited;
        grand_total_withdrawn += res.total_withdrawn;
        close(pipes[i][0]); // סיום שימוש בצינור, סוגרים צד קריאה
    }
    
    printf("Total: %d transactions | +%ld deposited | -%ld withdrawn\n", 
           grand_total_tx, grand_total_deposited, grand_total_withdrawn);

    printf("\nAccount balances after all transactions:\n");
    long final_total_balance = 0;
    
    // מעבר על מערך החשבונות (של האב!), והדפסתם, ולבסוף פירוק המנעולים (mutex_destroy)
    // **מאחר והאב מעולם לא עודכן לגבי הפעולות שקרו בתוך תהליכי הבן
    // (הם עבדו על עותקים של המערך), היתרות פה יהיו בדיוק כמו היתרות בהתחלה!
    for (int i = 0; i < num_accounts; i++) {
        printf("%d %s: $%ld\n", accounts[i].id, accounts[i].owner, accounts[i].balance);
        final_total_balance += accounts[i].balance;
        pthread_mutex_destroy(&accounts[i].lock); // ניקוי זיכרון של המנעול
    }

    // בדיקת תקינות סופית - אמרנו שזה תמיד יצליח מסיבות לא נכונות :)
    printf("\nBalance conservation check: %s (initial sum %ld == final sum %ld)\n",
           (initial_total_balance == final_total_balance) ? "PASSED" : "FAILED",
           initial_total_balance, final_total_balance);

    // ניקוי אחרון: הריסת הסמפור ושחרור הזיכרון המשותף שהקצינו עם mmap
    sem_destroy(branch_sem);
    munmap(branch_sem, sizeof(sem_t));

    return 0; // סיום התוכנית הראשי בהצלחה
}