#ifndef GARDENER_H
#define GARDENER_H

#include <stdbool.h>
#include "common.h"

typedef struct {
    int rank;
    int size;
    int G;
    int L;
    int D;
    int K;

    int lamport;
    int cycle;
    int acks_received;
    int heard_count;
    bool empty_fert_sent;
    bool waiting_for_restock;
    bool shop_needed;
    bool shopping_in_progress;

    int iterations_done;
    int target_iterations;
    int cs_ms;
    int shopping_ms;
    double cs_deadline;

    ProcessState state;
    bool is_shopkeeper;

    bool request_active;
    int my_req_clock;
    int my_req_cycle;

    bool *heard_from;
    Request *queue;
    int queue_len;
    int queue_cap;

    PendingAck *pending_ack;
    int pending_ack_len;
    int pending_ack_cap;
} Gardener;

void gardener_init(Gardener *g, int rank, int size, int G, int L, int D,
                   int target_iterations, int cs_ms, int shopping_ms);
void gardener_destroy(Gardener *g);

void gardener_start_request(Gardener *g);
void gardener_enter_weeding(Gardener *g);
void gardener_leave_weeding(Gardener *g);
void gardener_tick_weeding(Gardener *g);
void gardener_check_entry(Gardener *g);
void gardener_do_shopping(Gardener *g);

int gardener_handle_message(Gardener *g);

#endif /* GARDENER_H */
