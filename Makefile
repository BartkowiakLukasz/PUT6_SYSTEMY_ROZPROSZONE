CC      = mpicc
CFLAGS  = -Wall -Wextra -std=gnu11 -O2
LDFLAGS =
TARGET  = gardener
SRCS    = main.c gardener.c
OBJS    = $(SRCS:.c=.o)

.PHONY: all clean run

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $(OBJS) $(LDFLAGS)

%.o: %.c common.h gardener.h
	$(CC) $(CFLAGS) -c $< -o $@

run: $(TARGET)
	mpirun -np 5 ./$(TARGET) 2 2 2 2

clean:
	rm -f $(TARGET) $(OBJS)
