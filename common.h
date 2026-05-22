#ifndef COMMON_H
#define COMMON_H

#define MAX_PROCESSES 64

/* Tagi MPI */
#define TAG_REQ_TOOLS   1
#define TAG_ACK_TOOLS   2
#define TAG_EMPTY_FERT  3
#define TAG_RESTOCK     4
#define TAG_DEFER_TOOLS 5

#define SHOPKEEPER_RANK 0

typedef enum {
    STATE_REST,
    STATE_WAIT,
    STATE_WEEDING,
    STATE_SHOPPING
} ProcessState;

typedef struct {
    int clock;
    int pid;
    int cycle;
} Request;

typedef struct {
    int pid;
    int req_clock;
} PendingAck;

#define ANSI_COLOR_RESET "\x1b[0m"

static const char *const ANSI_COLORS[] = {
    "\x1b[31m", // Red
    "\x1b[32m", // Green
    "\x1b[33m", // Yellow
    "\x1b[34m", // Blue
    "\x1b[35m", // Magenta
    "\x1b[36m", // Cyan
    "\x1b[91m", // Bright Red
    "\x1b[92m", // Bright Green
    "\x1b[93m", // Bright Yellow
    "\x1b[94m", // Bright Blue
    "\x1b[95m", // Bright Magenta
    "\x1b[96m"  // Bright Cyan
};

#define GET_COLOR(rank) ANSI_COLORS[(rank) % 12]

#endif /* COMMON_H */
