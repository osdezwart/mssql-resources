SELECT TOP 10
ObjectName          = OBJECT_NAME(qt.objectid)
,DiskReads          = qs.total_physical_reads -- The worst reads, disk reads
,MemoryReads        = qs.total_logical_reads  --Logical Reads are memory reads
,Executions         = qs.execution_count
,AvgDuration        = qs.total_elapsed_time / qs.execution_count
,CPUTime            = qs.total_worker_time
,DiskWaitAndCPUTime = qs.total_elapsed_time
,MemoryWrites       = qs.max_logical_writes
,DateCached         = qs.creation_time
,DatabaseName       = DB_Name(qt.dbid)
FROM sys.dm_exec_query_stats AS qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS qt
WHERE qt.dbid = db_id() -- Filter by current database
ORDER BY qs.total_elapsed_time DESC
