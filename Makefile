CC = gcc
CFLAGS = -Wall -Wextra -pthread
TARGET = bank

all: $(TARGET)

$(TARGET): bank.c
	$(CC) $(CFLAGS) -o $(TARGET) bank.c

race: bank.c
	$(CC) $(CFLAGS) -DNO_LOCK -o $(TARGET) bank.c

# הרצה רגילה ובטוחה (עם מנעולים)
run: $(TARGET)
	./$(TARGET)

# הרצת מצב תחרות: מנקה אוטומטית, מקמפל בלי מנעולים ומריץ
run_race: clean race
	./$(TARGET)

clean:
	rm -f $(TARGET)