CC = gcc
CFLAGS = -Wall -Wextra -pthread
TARGET = bank

all: $(TARGET)

$(TARGET): bank.c
	$(CC) $(CFLAGS) -o $(TARGET) bank.c

race: bank.c
	$(CC) $(CFLAGS) -DNO_LOCK -o $(TARGET) bank.c

clean:
	rm -f $(TARGET)