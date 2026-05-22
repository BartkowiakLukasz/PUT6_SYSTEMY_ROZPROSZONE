#include "gardener.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <mpi.h>

static int req_priority_less(const Request *a, const Request *b)
{
    if (a->clock != b->clock) {
        return a->clock < b->clock;
    }
    return a->pid < b->pid;
}

static int requests_equal(const Request *a, const Request *b)
{
    return a->clock == b->clock && a->pid == b->pid && a->cycle == b->cycle;
}

static void sort_queue(Gardener *g)
{
    for (int i = 0; i < g->queue_len - 1; i++) {
        for (int j = i + 1; j < g->queue_len; j++) {
            if (!req_priority_less(&g->queue[i], &g->queue[j])) {
                Request tmp = g->queue[i];
                g->queue[i] = g->queue[j];
                g->queue[j] = tmp;
            }
        }
    }
}

static void queue_add(Gardener *g, int clock, int pid, int cycle)
{
    if (cycle < g->cycle) {
        return;
    }

    Request req = {.clock = clock, .pid = pid, .cycle = cycle};

    for (int i = 0; i < g->queue_len; i++) {
        if (requests_equal(&g->queue[i], &req)) {
            return;
        }
    }

    if (g->queue_len >= g->queue_cap) {
        g->queue_cap *= 2;
        g->queue = realloc(g->queue, (size_t)g->queue_cap * sizeof(Request));
    }

    g->queue[g->queue_len++] = req;
    sort_queue(g);
}

static void queue_clear(Gardener *g)
{
    g->queue_len = 0;
}

static void sync_cycle(Gardener *g, int new_cycle)
{
    if (new_cycle > g->cycle) {
        g->cycle = new_cycle;
        queue_clear(g);
    }
}

static void pending_ack_add(Gardener *g, int pid, int req_clock)
{
    for (int i = 0; i < g->pending_ack_len; i++) {
        if (g->pending_ack[i].pid == pid &&
            g->pending_ack[i].req_clock == req_clock) {
            return;
        }
    }

    if (g->pending_ack_len >= g->pending_ack_cap) {
        g->pending_ack_cap *= 2;
        g->pending_ack = realloc(g->pending_ack,
                                 (size_t)g->pending_ack_cap * sizeof(PendingAck));
    }

    g->pending_ack[g->pending_ack_len].pid = pid;
    g->pending_ack[g->pending_ack_len].req_clock = req_clock;
    g->pending_ack_len++;
}

static void lamport_send(Gardener *g)
{
    g->lamport++;
}

static void lamport_recv(Gardener *g, int msg_clock)
{
    g->lamport = (g->lamport > msg_clock ? g->lamport : msg_clock) + 1;
}

static void mark_heard(Gardener *g, int from)
{
    if (from == g->rank) {
        return;
    }

    if (!g->heard_from[from]) {
        g->heard_from[from] = true;
        g->heard_count++;
    }
}

static void reset_heard(Gardener *g)
{
    memset(g->heard_from, 0, (size_t)g->size * sizeof(bool));
    g->heard_count = 0;
}

static void send_req_tools(Gardener *g, int dest)
{
    int payload[3] = {g->my_req_clock, g->rank, g->my_req_cycle};

    MPI_Send(payload, 3, MPI_INT, dest, TAG_REQ_TOOLS, MPI_COMM_WORLD);
    fprintf(stderr, "%s[P%d] [%d] SEND REQ_TOOLS(clock=%d, pid=%d, cycle=%d) -> P%d%s\n",
            GET_COLOR(g->rank), g->rank, g->lamport, payload[0], g->rank, g->my_req_cycle, dest, ANSI_COLOR_RESET);
}

static void broadcast_req_tools(Gardener *g)
{
    for (int i = 0; i < g->size; i++) {
        if (i != g->rank) {
            send_req_tools(g, i);
        }
    }
}

static void send_ack_tools(Gardener *g, int dest, int req_clock, int req_pid)
{
    int payload[2] = {req_clock, req_pid};
    lamport_send(g);

    MPI_Send(payload, 2, MPI_INT, dest, TAG_ACK_TOOLS, MPI_COMM_WORLD);
    fprintf(stderr,
            "%s[P%d] [%d] SEND ACK_TOOLS(clock=%d, pid=%d) -> P%d%s\n",
            GET_COLOR(g->rank), g->rank, g->lamport, req_clock, req_pid, dest, ANSI_COLOR_RESET);
}

static void send_defer_tools(Gardener *g, int dest, int req_clock, int req_pid)
{
    int payload[3] = {g->lamport, req_clock, req_pid};
    lamport_send(g);
    MPI_Send(payload, 3, MPI_INT, dest, TAG_DEFER_TOOLS, MPI_COMM_WORLD);
    fprintf(stderr,
            "%s[P%d] [%d] SEND DEFER_TOOLS(clock=%d, pid=%d) -> P%d%s\n",
            GET_COLOR(g->rank), g->rank, g->lamport, req_clock, req_pid, dest, ANSI_COLOR_RESET);
}

static void send_empty_fert(Gardener *g)
{
    lamport_send(g);
    // Wysyłamy: Zegar, PID (rank), Cykl
    int payload[3] = {g->lamport, g->rank, g->cycle};
    
    MPI_Send(payload, 3, MPI_INT, SHOPKEEPER_RANK, TAG_EMPTY_FERT, MPI_COMM_WORLD);
    
    fprintf(stderr, "%s[P%d] [%d] SEND EMPTY_FERT(cykl=%d) -> Kupiec%s\n", 
            GET_COLOR(g->rank), g->rank, g->lamport, g->cycle, ANSI_COLOR_RESET);
}

static void broadcast_restock(Gardener *g, int new_cycle)
{
    lamport_send(g);
    for (int i = 0; i < g->size; i++) {
        if (i != g->rank) {
            MPI_Send(&new_cycle, 1, MPI_INT, i, TAG_RESTOCK, MPI_COMM_WORLD);
        }
    }
    fprintf(stderr, "%s[Kupiec P%d] [%d] SEND RESTOCK(cycle=%d) -> wszyscy%s\n",
            GET_COLOR(g->rank), g->rank, g->lamport, new_cycle, ANSI_COLOR_RESET);
}

static int their_req_has_higher_priority(Gardener *g, int their_clock, int their_pid, int their_cycle)
{
    if (!g->request_active) {
        return true;
    }
    if (their_cycle != g->my_req_cycle) {
        return their_cycle > g->my_req_cycle;
    }
    if (their_clock != g->my_req_clock) {
        return their_clock < g->my_req_clock;
    }
    return their_pid < g->rank;
}

static int my_queue_index(Gardener *g)
{
    Request mine = {
        .clock = g->my_req_clock,
        .pid = g->rank,
        .cycle = g->my_req_cycle,
    };

    for (int i = 0; i < g->queue_len; i++) {
        if (requests_equal(&g->queue[i], &mine)) {
            return i;
        }
    }
    return -1;
}

static void handle_restock(Gardener *g, int new_cycle)
{
    bool was_waiting = (g->state == STATE_WAIT);

    sync_cycle(g, new_cycle);
    g->empty_fert_sent = false;
    g->waiting_for_restock = false;

    fprintf(stderr, "%s[P%d] [%d] RECV RESTOCK(cycle=%d), stan=%d%s\n",
            GET_COLOR(g->rank), g->rank, g->lamport, new_cycle, g->state, ANSI_COLOR_RESET);

    if (was_waiting && g->request_active) {
        g->lamport++;
        g->acks_received = 0;
        reset_heard(g);
        g->my_req_clock = g->lamport;
        g->my_req_cycle = g->cycle;
        queue_add(g, g->my_req_clock, g->rank, g->my_req_cycle);
        broadcast_req_tools(g);
        fprintf(stderr,
                "%s[P%d] [%d] Ponowne REQ_TOOLS po RESTOCK (clock=%d, cycle=%d)%s\n",
                GET_COLOR(g->rank), g->rank, g->lamport, g->my_req_clock, g->my_req_cycle, ANSI_COLOR_RESET);
    }
}

static void start_shopping(Gardener *g)
{
    if (g->shopping_in_progress) {
        return;
    }

    g->shopping_in_progress = true;
    g->state = STATE_SHOPPING;
    g->request_active = false;
    fprintf(stderr, "%s[Kupiec P%d] [%d] Przechodzi w SHOPPING%s\n", GET_COLOR(g->rank), g->rank, g->lamport, ANSI_COLOR_RESET);
}

static void handle_req_tools(Gardener *g, int from, int clock, int pid, int cycle)
{
    lamport_recv(g, clock);
    mark_heard(g, from);

    if (cycle < g->cycle) {
        fprintf(stderr,
                "%s[P%d] [%d] IGNORE REQ_TOOLS od P%d (stary cykl %d < %d)%s\n",
                GET_COLOR(g->rank), g->rank, g->lamport, from, cycle, g->cycle, ANSI_COLOR_RESET);
        return;
    }

    sync_cycle(g, cycle);
    queue_add(g, clock, pid, cycle);

    fprintf(stderr,
            "%s[P%d] [%d] RECV REQ_TOOLS(clock=%d, pid=%d, cycle=%d) od P%d%s\n",
            GET_COLOR(g->rank), g->rank, g->lamport, clock, pid, cycle, from, ANSI_COLOR_RESET);

    bool send_now = false;

    if (g->state == STATE_REST) {
        send_now = true;
    } else if (g->state == STATE_WAIT && g->request_active) {
        send_now = their_req_has_higher_priority(g, clock, pid, cycle);
    }

    if (send_now) {
        send_ack_tools(g, from, clock, pid);
    } else if (g->state == STATE_WEEDING ||
               (g->state == STATE_WAIT && g->request_active &&
                !their_req_has_higher_priority(g, clock, pid, cycle))) {
        pending_ack_add(g, from, clock);
        fprintf(stderr, "%s[P%d] [%d] Odklada ACK dla P%d%s\n", GET_COLOR(g->rank), g->rank, g->lamport, from, ANSI_COLOR_RESET);
        send_defer_tools(g, from, clock, pid);
    }
}

static void handle_ack_tools(Gardener *g, int from, int req_clock, int req_pid)
{
    lamport_recv(g, req_clock);
    mark_heard(g, from);

    fprintf(stderr,
            "%s[P%d] [%d] RECV ACK_TOOLS(clock=%d, pid=%d) od P%d%s\n",
            GET_COLOR(g->rank), g->rank, g->lamport, req_clock, req_pid, from, ANSI_COLOR_RESET);

    if (g->state == STATE_WAIT && g->request_active &&
        req_pid == g->rank && req_clock == g->my_req_clock) {
        g->acks_received++;
    }
}

static void handle_defer_tools(Gardener *g, int from, int msg_clock, int req_clock, int req_pid)
{
    lamport_recv(g, msg_clock);
    mark_heard(g, from);

    fprintf(stderr,
            "%s[P%d] [%d] RECV DEFER_TOOLS(clock=%d, pid=%d) od P%d%s\n",
            GET_COLOR(g->rank), g->rank, g->lamport, req_clock, req_pid, from, ANSI_COLOR_RESET);
}

static void handle_empty_fert(Gardener *g, int msg_clock, int from, int msg_cycle)
{
    lamport_recv(g, msg_clock);

    if (!g->is_shopkeeper) {
        return;
    }

    fprintf(stderr, "%s[Kupiec P%d] [%d] RECV EMPTY_FERT(cykl=%d) od P%d%s\n", 
            GET_COLOR(g->rank), g->rank, g->lamport, msg_cycle, from, ANSI_COLOR_RESET);

    // KLUCZOWA POPRAWKA: Ignorujemy powiadomienia ze starych cykli!
    if (msg_cycle < g->cycle) {
        fprintf(stderr, "%s[Kupiec P%d] [%d] IGNORUJE EMPTY_FERT od P%d (stary cykl %d < %d)%s\n", 
                GET_COLOR(g->rank), g->rank, g->lamport, from, msg_cycle, g->cycle, ANSI_COLOR_RESET);
        return;
    }

    if (g->state == STATE_WEEDING) {
        g->shop_needed = true;
    } else {
        start_shopping(g);
    }
}

void gardener_init(Gardener *g, int rank, int size, int G, int L, int D,
                   int target_iterations, int cs_ms, int shopping_ms)
{
    memset(g, 0, sizeof(*g));
    g->rank = rank;
    g->size = size;
    g->G = G;
    g->L = L;
    g->D = D;
    g->K = G < L ? G : L;
    g->target_iterations = target_iterations;
    g->cs_ms = cs_ms;
    g->shopping_ms = shopping_ms;
    g->state = STATE_REST;
    g->is_shopkeeper = (rank == SHOPKEEPER_RANK);
    g->queue_cap = 16;
    g->pending_ack_cap = 16;

    g->heard_from = calloc((size_t)size, sizeof(bool));
    g->queue = malloc((size_t)g->queue_cap * sizeof(Request));
    g->pending_ack = malloc((size_t)g->pending_ack_cap * sizeof(PendingAck));

    if (!g->heard_from || !g->queue || !g->pending_ack) {
        fprintf(stderr, "%s[P%d] [0] Brak pamieci%s\n", GET_COLOR(rank), rank, ANSI_COLOR_RESET);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
}

void gardener_destroy(Gardener *g)
{
    free(g->heard_from);
    free(g->queue);
    free(g->pending_ack);
}

void gardener_start_request(Gardener *g)
{
    if (g->state != STATE_REST) {
        return;
    }

    g->lamport++;
    g->my_req_clock = g->lamport;
    g->my_req_cycle = g->cycle;
    g->acks_received = 0;
    g->empty_fert_sent = false;
    g->waiting_for_restock = false;
    reset_heard(g);

    g->request_active = true;
    queue_add(g, g->my_req_clock, g->rank, g->my_req_cycle);
    broadcast_req_tools(g);
    g->state = STATE_WAIT;

    fprintf(stderr,
            "%s[P%d] [%d] REST -> WAIT (clock=%d, cycle=%d)%s\n",
            GET_COLOR(g->rank), g->rank, g->lamport, g->my_req_clock, g->my_req_cycle, ANSI_COLOR_RESET);
}

void gardener_enter_weeding(Gardener *g)
{
    g->state = STATE_WEEDING;
    g->cs_deadline = MPI_Wtime() + (double)g->cs_ms / 1000.0;
    printf("%s[%d] [%d] Jestem w sekcji krytycznej.%s\n", GET_COLOR(g->rank), g->rank, g->lamport, ANSI_COLOR_RESET);
    fflush(stdout);
}

void gardener_tick_weeding(Gardener *g)
{
    if (g->state != STATE_WEEDING) {
        return;
    }
    if (MPI_Wtime() >= g->cs_deadline) {
        gardener_leave_weeding(g);
    }
}

void gardener_leave_weeding(Gardener *g)
{
    for (int i = 0; i < g->pending_ack_len; i++) {
        send_ack_tools(g, g->pending_ack[i].pid, g->pending_ack[i].req_clock,
                       g->pending_ack[i].pid);
    }
    g->pending_ack_len = 0;

    g->request_active = false;
    g->state = STATE_REST;
    g->iterations_done++;

    fprintf(stderr, "%s[P%d] [%d] WEEDING -> REST (iteracja %d/%d)%s\n",
            GET_COLOR(g->rank), g->rank, g->lamport, g->iterations_done, g->target_iterations, ANSI_COLOR_RESET);

    if (g->is_shopkeeper && g->shop_needed && !g->shopping_in_progress) {
        start_shopping(g);
    }
}

void gardener_check_entry(Gardener *g)
{
    if (g->state != STATE_WAIT || !g->request_active || g->waiting_for_restock) {
        return;
    }

    int acks_needed = g->size - g->K;
    if (g->acks_received < acks_needed) {
        return;
    }

    if (g->heard_count < g->size - 1) {
        return;
    }

    int idx = my_queue_index(g);
    if (idx < 0) {
        return;
    }

    if (idx >= g->D) {
        g->waiting_for_restock = true;
        if (!g->empty_fert_sent) {
            g->empty_fert_sent = true;
            if (g->is_shopkeeper) {
                start_shopping(g);
            } else {
                send_empty_fert(g);
            }
        }
        return;
    }

    gardener_enter_weeding(g);
}

void gardener_do_shopping(Gardener *g)
{
    if (!g->is_shopkeeper || g->state != STATE_SHOPPING) {
        return;
    }

    fprintf(stderr, "%s[Kupiec P%d] [%d] Zakupy (%d ms)...%s\n", GET_COLOR(g->rank), g->rank, g->lamport, g->shopping_ms, ANSI_COLOR_RESET);
    usleep((useconds_t)g->shopping_ms * 1000U);

    int new_cycle = g->cycle + 1;
    g->cycle = new_cycle;
    queue_clear(g);
    broadcast_restock(g, new_cycle);

    g->shopping_in_progress = false;
    g->shop_needed = false;
    g->state = STATE_REST;
    g->empty_fert_sent = false;
    g->waiting_for_restock = false;

    fprintf(stderr, "%s[Kupiec P%d] [%d] SHOPPING -> REST (cykl=%d)%s\n",
            GET_COLOR(g->rank), g->rank, g->lamport, new_cycle, ANSI_COLOR_RESET);
}

int gardener_handle_message(Gardener *g)
{
    int flag = 0;
    MPI_Status status;

    MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, &status);
    if (!flag) {
        return 0;
    }

    int tag = status.MPI_TAG;
    int from = status.MPI_SOURCE;

    if (tag == TAG_REQ_TOOLS) {
        int payload[3];
        MPI_Recv(payload, 3, MPI_INT, from, TAG_REQ_TOOLS, MPI_COMM_WORLD,
                 MPI_STATUS_IGNORE);
        handle_req_tools(g, from, payload[0], payload[1], payload[2]);
    } else if (tag == TAG_ACK_TOOLS) {
        int payload[2];
        MPI_Recv(payload, 2, MPI_INT, from, TAG_ACK_TOOLS, MPI_COMM_WORLD,
                 MPI_STATUS_IGNORE);
        handle_ack_tools(g, from, payload[0], payload[1]);
    } else if (tag == TAG_EMPTY_FERT) {
        int payload[3];
        MPI_Recv(payload, 3, MPI_INT, from, TAG_EMPTY_FERT, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        // payload[0] = zegar, payload[1] = PID nadawcy, payload[2] = cykl
        handle_empty_fert(g, payload[0], payload[1], payload[2]);
    } else if (tag == TAG_DEFER_TOOLS) {
        int payload[3];
        MPI_Recv(payload, 3, MPI_INT, from, TAG_DEFER_TOOLS, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        handle_defer_tools(g, from, payload[0], payload[1], payload[2]);
    } else if (tag == TAG_RESTOCK) {
        int new_cycle;
        MPI_Recv(&new_cycle, 1, MPI_INT, from, TAG_RESTOCK, MPI_COMM_WORLD,
                 MPI_STATUS_IGNORE);
        lamport_recv(g, new_cycle);
        handle_restock(g, new_cycle);
    } else {
        fprintf(stderr, "%s[P%d] [%d] Nieznany tag %d od P%d%s\n", GET_COLOR(g->rank), g->rank, g->lamport, tag, from, ANSI_COLOR_RESET);
        MPI_Recv(MPI_BOTTOM, 0, MPI_BYTE, from, tag, MPI_COMM_WORLD,
                 MPI_STATUS_IGNORE);
    }

    return 1;
}
