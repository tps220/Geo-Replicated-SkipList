# Geo-Replicated-SkipList

### Startup Process
1) Transfer relevant parameters (number of threads, initial size, number of numa zones) into global store
2) Initialize memory management (allocator, hazard pointer manager)
3) Initialize data layer with two sentinels
4) Initialize search layer
     i) construct allocator for zone
     ii) insert sentinel nodes
     iii) attach job queue for updates (separated onto own thread)
     iv) attach memory queue for garbage collection (separated onto own numa local thread)
     v) set the sleep time for helper threads
     vi) assign a unique id based upon numa zone #
5) Start data layer helper, which has the focus of traversing across the data layer to identify and propogate updates
6) Start search layer helper(s), which await their job queues to execute updates
7) Add initial data into the system based upon size parameter, allocated equally across all nodes; the program waits until data has been fully replicated and all search layers have the same size as the data layer
8) create n threads, and thus n * (k = 2) hazard nodes and await to start test – utilizing a memory barrier – until all threads have been properly intialized and spawned
9) Release the threads and execute benchmarks / application
10) Cleanup after exit triggered
