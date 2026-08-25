// A simulated free-list heap allocator (first-fit and best-fit variants)
// that tracks external fragmentation as a synthetic workload of variable-
// size allocations and frees runs against it. This models the memory-
// management side of a job scheduler: real schedulers have to reason
// about fragmentation when packing jobs of different memory footprints
// onto a fixed-size pool, and this is the underlying mechanism.
//
// Build: gcc -O2 -o allocator allocator.c
// Run:   ./allocator [heap_size_bytes] [num_operations] [seed]
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct Block {
    size_t offset;
    size_t size;
    int free;           // 1 if this block is free, 0 if allocated
    int alloc_id;        // which allocation owns this block, -1 if free
    struct Block* next;
} Block;

typedef enum { FIRST_FIT, BEST_FIT } Strategy;

typedef struct {
    Block* head;
    size_t heap_size;
    Strategy strategy;
    int next_alloc_id;
} Heap;

static Heap* heap_create(size_t size, Strategy strategy) {
    Heap* h = malloc(sizeof(Heap));
    h->head = malloc(sizeof(Block));
    h->head->offset = 0;
    h->head->size = size;
    h->head->free = 1;
    h->head->alloc_id = -1;
    h->head->next = NULL;
    h->heap_size = size;
    h->strategy = strategy;
    h->next_alloc_id = 0;
    return h;
}

// find_fit returns the block to place an allocation of `size` into,
// according to the heap's configured strategy, or NULL if nothing fits.
static Block* find_fit(Heap* h, size_t size) {
    Block* best = NULL;
    for (Block* b = h->head; b; b = b->next) {
        if (!b->free || b->size < size) continue;
        if (h->strategy == FIRST_FIT) {
            return b;
        }
        // BEST_FIT: keep the smallest block that's still big enough,
        // which minimizes leftover fragment size per allocation.
        if (best == NULL || b->size < best->size) {
            best = b;
        }
    }
    return best;
}

// alloc splits the chosen free block into an allocated piece of exactly
// `size` bytes and a remaining free piece (if any leftover space exists).
// Returns the alloc_id on success, -1 if no block fit.
static int heap_alloc(Heap* h, size_t size) {
    Block* b = find_fit(h, size);
    if (!b) return -1;

    if (b->size > size) {
        Block* remainder = malloc(sizeof(Block));
        remainder->offset = b->offset + size;
        remainder->size = b->size - size;
        remainder->free = 1;
        remainder->alloc_id = -1;
        remainder->next = b->next;
        b->next = remainder;
        b->size = size;
    }
    b->free = 0;
    b->alloc_id = h->next_alloc_id++;
    return b->alloc_id;
}

// heap_free marks the block owning alloc_id as free, then coalesces it
// with adjacent free blocks to fight fragmentation - this is the step a
// naive allocator skips, and skipping it is what causes fragmentation to
// spiral under a long-running workload.
static void heap_free(Heap* h, int alloc_id) {
    Block* prev = NULL;
    for (Block* b = h->head; b; prev = b, b = b->next) {
        if (b->alloc_id != alloc_id) continue;
        b->free = 1;
        b->alloc_id = -1;

        // Coalesce with next block if it's also free.
        if (b->next && b->next->free) {
            Block* dead = b->next;
            b->size += dead->size;
            b->next = dead->next;
            free(dead);
        }
        // Coalesce with previous block if it's also free.
        if (prev && prev->free) {
            prev->size += b->size;
            prev->next = b->next;
            free(b);
        }
        return;
    }
}

// external_fragmentation_pct returns the fraction of *free* memory that is
// unusable because it's split across many small blocks rather than one
// large one - the standard external-fragmentation metric.
static double external_fragmentation_pct(Heap* h) {
    size_t total_free = 0, largest_free = 0, free_blocks = 0;
    for (Block* b = h->head; b; b = b->next) {
        if (!b->free) continue;
        total_free += b->size;
        free_blocks++;
        if (b->size > largest_free) largest_free = b->size;
    }
    if (total_free == 0) return 0.0;
    return 100.0 * (1.0 - (double)largest_free / (double)total_free);
}

static int count_free_blocks(Heap* h) {
    int n = 0;
    for (Block* b = h->head; b; b = b->next) if (b->free) n++;
    return n;
}

static void heap_destroy(Heap* h) {
    Block* b = h->head;
    while (b) { Block* next = b->next; free(b); b = next; }
    free(h);
}

// run_workload allocates and frees random-sized blocks for `ops`
// iterations, printing fragmentation every `report_every` operations.
static void run_workload(Strategy strategy, size_t heap_size, int ops, unsigned seed) {
    printf("\n--- strategy: %s ---\n", strategy == FIRST_FIT ? "first-fit" : "best-fit");
    Heap* h = heap_create(heap_size, strategy);
    srand(seed);

    int live_ids[4096];
    int live_count = 0;
    int failed_allocs = 0;

    for (int i = 0; i < ops; i++) {
        // 70% chance to allocate (if we have room in the tracking array),
        // 30% chance to free a live allocation, biasing toward growth
        // early and steady-state churn later - a reasonably realistic
        // pattern for a job scheduler's memory pool.
        int do_alloc = (live_count == 0) || (rand() % 10 < 7 && live_count < 4096);

        if (do_alloc) {
            size_t size = 64 + (rand() % 4096); // 64B - 4160B allocations
            int id = heap_alloc(h, size);
            if (id < 0) {
                failed_allocs++;
            } else {
                live_ids[live_count++] = id;
            }
        } else {
            int idx = rand() % live_count;
            heap_free(h, live_ids[idx]);
            live_ids[idx] = live_ids[--live_count];
        }

        if ((i + 1) % (ops / 5) == 0) {
            printf("  after %5d ops: free_blocks=%3d  external_frag=%.1f%%  failed_allocs_so_far=%d\n",
                   i + 1, count_free_blocks(h), external_fragmentation_pct(h), failed_allocs);
        }
    }

    printf("  final: free_blocks=%d  external_frag=%.1f%%  total_failed_allocs=%d\n",
           count_free_blocks(h), external_fragmentation_pct(h), failed_allocs);
    heap_destroy(h);
}

int main(int argc, char** argv) {
    size_t heap_size = argc > 1 ? (size_t)atol(argv[1]) : (size_t)(4 * 1024 * 1024); // 4MB default
    int ops = argc > 2 ? atoi(argv[2]) : 20000;
    unsigned seed = argc > 3 ? (unsigned)atoi(argv[3]) : 42;

    printf("heap_size=%zu bytes, ops=%d, seed=%u\n", heap_size, ops, seed);
    run_workload(FIRST_FIT, heap_size, ops, seed);
    run_workload(BEST_FIT, heap_size, ops, seed);
    return 0;
}
