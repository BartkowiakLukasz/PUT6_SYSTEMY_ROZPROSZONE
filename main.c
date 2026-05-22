#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <mpi.h>

#include "gardener.h"

static void usage(const char *prog, int rank)
{
    fprintf(stderr,
            "%s[P%d] [0] Uzycie: mpirun -np N %s G L D [iteracje] [shopping_ms] [cs_ms]\n"
            "  G, L     - liczba grabek i lopatek (K = min(G,L))\n"
            "  D        - nawoz na cykl\n"
            "  iteracje - ile razy kazdy proces wchodzi do szklarni (domyslnie 2)\n"
            "  shopping_ms - czas zakupow Kupca (domyslnie 300)\n"
            "  cs_ms    - czas pracy w sekcji krytycznej (domyslnie 200)\n"
            "\nPrzyklad: mpirun -np 5 %s 2 2 2 2%s\n",
            GET_COLOR(rank), rank, prog, prog, ANSI_COLOR_RESET);
}

static int all_done(const Gardener *g)
{
    int local = (g->iterations_done >= g->target_iterations) ? 1 : 0;
    int global = 0;
    MPI_Allreduce(&local, &global, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
    return global;
}

int main(int argc, char **argv)
{
    int provided = 0;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_SINGLE, &provided);

    int rank = 0;
    int size = 0;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (argc < 4) {
        if (rank == 0) {
            usage(argv[0], rank);
        }
        MPI_Finalize();
        return 1;
    }

    if (size < 2) {
        if (rank == 0) {
            fprintf(stderr, "%s[P%d] [0] Wymagane co najmniej 2 procesy MPI.%s\n", GET_COLOR(rank), rank, ANSI_COLOR_RESET);
        }
        MPI_Finalize();
        return 1;
    }

    if (size > MAX_PROCESSES) {
        if (rank == 0) {
            fprintf(stderr, "%s[P%d] [0] Maksymalnie %d procesow.%s\n", GET_COLOR(rank), rank, MAX_PROCESSES, ANSI_COLOR_RESET);
        }
        MPI_Finalize();
        return 1;
    }

    int G = atoi(argv[1]);
    int L = atoi(argv[2]);
    int D = atoi(argv[3]);
    int iterations = (argc > 4) ? atoi(argv[4]) : 2;
    int shopping_ms = (argc > 5) ? atoi(argv[5]) : 300;
    int cs_ms = (argc > 6) ? atoi(argv[6]) : 200;

    if (G < 1 || L < 1 || D < 1 || iterations < 1) {
        if (rank == 0) {
            fprintf(stderr, "%s[P%d] [0] G, L, D i iteracje musza byc >= 1.%s\n", GET_COLOR(rank), rank, ANSI_COLOR_RESET);
        }
        MPI_Finalize();
        return 1;
    }

    Gardener g;
    gardener_init(&g, rank, size, G, L, D, iterations, cs_ms, shopping_ms);

    if (rank == 0) {
        int K = G < L ? G : L;
        fprintf(stderr,
                "%s[P%d] [%d] === Algorytm Ogrodnikow: N=%d, G=%d, L=%d, D=%d, K=%d, "
                "iter=%d ===%s\n",
                GET_COLOR(rank), rank, g.lamport, size, G, L, D, K, iterations, ANSI_COLOR_RESET);
        fprintf(stderr, "%s[P%d] [%d] Kupiec: P%d%s\n", GET_COLOR(rank), rank, g.lamport, SHOPKEEPER_RANK, ANSI_COLOR_RESET);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    int request_delay_ms = 50 + (rank > 0 ? rank * 40 : 0);

    while (!all_done(&g)) {
        while (gardener_handle_message(&g)) {
        }

        if (g.state == STATE_SHOPPING) {
            gardener_do_shopping(&g);
            continue;
        }

        if (g.state == STATE_WEEDING) {
            gardener_tick_weeding(&g);
            continue;
        }

        if (g.state == STATE_WAIT) {
            while (gardener_handle_message(&g)) {
            }
            gardener_check_entry(&g);
        }

        if (g.state == STATE_REST && g.iterations_done < g.target_iterations &&
            !g.shopping_in_progress) {
            
            // --- AKTYWNE CZEKANIE ---
            double start_wait = MPI_Wtime();
            double wait_sec = request_delay_ms / 1000.0;
            
            // Zamiast spać, kręcimy się w kółko przez np. 450ms, 
            // cały czas opróżniając bufor z wiadomościami od szybkich kolegów!
            while (MPI_Wtime() - start_wait < wait_sec) {
                while (gardener_handle_message(&g)) {
                    // wyciągamy wiadomości, żeby nie zapchać sieci
                }
                usleep(100); // 0.1 ms drzemki, żeby procesor nie wybuchł (nie 100%)
            }
            // ------------------------

            gardener_start_request(&g);
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);

    if (rank == 0) {
        fprintf(stderr, "%s[P%d] [%d] === Symulacja zakonczona ===%s\n", GET_COLOR(rank), rank, g.lamport, ANSI_COLOR_RESET);
    }

    gardener_destroy(&g);
    MPI_Finalize();
    return 0;
}
