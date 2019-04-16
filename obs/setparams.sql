alter system set max_worker_processes = 24;
alter system set max_parallel_workers_per_gather = 24;
alter system set max_parallel_workers = 24;

/* change these to fit your memory size */
alter system set enable_seqscan = off;
alter system set shared_buffers = "25GB";
alter system set work_mem = "2GB";
