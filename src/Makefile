ROOT = ../

include $(ROOT)/common/Makefile.common

BINS = $(BINDIR)/lazy-numa
EXTRA_PATHS = -I./Architecture/Allocator -I./Architecture/Nodes -I./Architecture/Queues -I./Platform/MemoryReclamation -I./Platform/SearchLayer -I$(ROOT)/common/atomic_ops
EXTRA_FLAGS = -lpthread -g
.PHONY:	all clean

all:	main

Allocator.o: ./Architecture/Allocator/Allocator.h
	$(CC) $(CFLAGS) $(EXTRA_PATHS) -c -o $(BUILDIR)/Allocator.o ./Architecture/Allocator/Allocator.c

Nodes.o: ./Architecture/Nodes/Nodes.h ./Architecture/Allocator/Allocator.h
	$(CC) $(CFLAGS) $(EXTRA_PATHS) $(EXTRA_FLAGS) -c -o $(BUILDIR)/Nodes.o ./Architecture/Nodes/Nodes.c

JobQueue.o: ./Architecture/Queues/JobQueue.h ./Architecture/Nodes/Nodes.h ./Architecture/Allocator/Allocator.h
	$(CC) $(CFLAGS) $(EXTRA_PATHS) $(EXTRA_FLAGS) -c -o $(BUILDIR)/JobQueue.o ./Architecture/Queues/JobQueue.c

LinkedList.o: ./Platform/MemoryReclamation/LinkedList.h ./Architecture/Nodes/Nodes.h
	$(CC) $(CFLAGS) $(EXTRA_PATHS) -c -o $(BUILDIR)/LinkedList.o ./Platform/MemoryReclamation/LinkedList.c

Hazard.o: ./Platform/MemoryReclamation/Hazard.h ./Architecture/Nodes/Nodes.h
	$(CC) $(CFLAGS) $(EXTRA_PATHS) -c -o $(BUILDIR)/Hazard.o ./Platform/MemoryReclamation/Hazard.c

SkipListLazyLock.o: ./Platform/SearchLayer/SkipListLazyLock.h ./Architecture/Nodes/Nodes.h
	$(CC) $(CFLAGS) $(EXTRA_PATHS) $(EXTRA_FLAGS) -c -o $(BUILDIR)/SkipListLazyLock.o ./Platform/SearchLayer/SkipListLazyLock.c

SearchLayer.o: ./Platform/SearchLayer/SearchLayer.h ./Architecture/Nodes/Nodes.h ./Architecture/Queues/JobQueue.h
	$(CC) $(CFLAGS) $(EXTRA_PATHS) $(EXTRA_FLAGS) -c -o $(BUILDIR)/SearchLayer.o ./Platform/SearchLayer/SearchLayer.c

DataLayer.o: DataLayer.h ./Platform/SearchLayer/SearchLayer.h ./Architecture/Nodes/Nodes.h ./Architecture/Queues/JobQueue.h
	$(CC) $(CFLAGS) $(EXTRA_PATHS) $(EXTRA_FLAGS) -c -o $(BUILDIR)/DataLayer.o DataLayer.c

Set.o: Set.h DataLayer.h ./Architecture/Allocator/Allocator.h
	$(CC) $(CFLAGS) $(EXTRA_PATHS) $(EXTRA_FLAGS) -c -o $(BUILDIR)/Set.o Set.c

test.o: test.c Set.h
	$(CC) $(CFLAGS) $(EXTRA_PATHS) $(EXTRA_FLAGS) -c -o $(BUILDIR)/test.o test.c

main: Allocator.o Nodes.o JobQueue.o SkipListLazyLock.o SearchLayer.o DataLayer.o Set.o test.o Hazard.o LinkedList.o
	$(CC) $(CFLAGS) $(EXTRA_PATHS) $(EXTRA_FLAGS) $(BUILDIR)/Allocator.o $(BUILDIR)/Nodes.o $(BUILDIR)/JobQueue.o $(BUILDIR)/LinkedList.o $(BUILDIR)/Hazard.o $(BUILDIR)/SkipListLazyLock.o $(BUILDIR)/SearchLayer.o $(BUILDIR)/DataLayer.o $(BUILDIR)/Set.o $(BUILDIR)/test.o -o $(BINS) $(LDFLAGS) -lnuma

clean:
	-rm -f $(BINS)
