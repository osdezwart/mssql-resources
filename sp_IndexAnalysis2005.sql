USE master
GO

IF OBJECT_ID('dbo.sp_IndexAnalysis2005') IS NOT NULL
    DROP PROCEDURE dbo.sp_IndexAnalysis2005
GO

/********************************************************************************************* 
Index Analysis Script v1.00 
(C) 2010, Jason Strate

Feedback:
    mailto:jasonstrate@gmail.com 
    http://www.jasonstrate.com

License: 
   This query is free to download and use for personal, educational, and internal 
   corporate purposes, provided that this header is preserved. Redistribution or sale 
   of this query, in whole or in part, is prohibited without the author's express 
   written consent. 

Note: 
    Populating the table name in the function for the @ObjectID variable is optional.  If 
    the value of @ObjectID is NULL then information on all tables is returned. 

    Recommendations in the Index Action column are not black and white recommendations.  
    They are more light grey ideas of what may be appropriate.  Always use your experience 
    with the database in place of a blanket recommendation. 

    The information in the DMVs is gathered from when the SQL Server service last started or 
    when the database was last created, which ever event is more recent. 

    The index statistics accumulated in sys.dm_db_index_operational_stats and 
    sys.dm_db_index_usage_stats are reset when the index is rebuilt. 

    The index statistics for a table that are accumulated in the DMVs 
    sys.dm_db_missing_index_* are reset whenever an index is created on the table. 

    The index name provided in the name column for indexes that do not exist is not a 
    recommended name for the index.  It’s just an informative placeholder. 
   
Columns:
    row_id: Row identifier used for populating the table 
    index_action: Analysis recommendation on action to take on the index 
        CREATE: Recommend adding the index to the table. 
        DROP: Recommend dropping the index from the table 
        BLEND: Review the missing index details to see if the missing index details can be 
            added to an existing index. 
        REALIGN: Bookmark lookups on the index exceed the number of seeks on the table.  
            Recommend investigating whether to move the clustered index to another index or 
            add included columns to the indexes that are part of the bookmark lookups. 
    pros: list of reasons that indicate the benefits provided by the index 
        FK: The index schema maps to a foreign key 
        UQ: Index is a unique constraint 
        $, $$, $$$, $$$+: Indicates the ratio of read to write uses in execution plans.  The 
            higher the ratio the more dollar signs; this should correlate to greater benefit 
            provided by the index. 
    cons: list of reasons that indicate some negative aspects associate with the index 
        SCN: Flag indicating that the ratio of seeks to scans on the index exceeds 1,000. 
        DP: Index schema is a duplicate of another index. 
        OV: Index schema overlaps another index. 
        $, $$, $$$, $$$+: Indicates the ratio of write to read uses in execution plans.  The 
            higher the ratio the more dollar signs; this should correlate to greater benefit 
            provided by the index. 
    filegroup: file group that the index is located.
    schema_id: Schema ID 
    schema_name: Name of the schema. 
    object_id: Object ID 
    table_name: Name of the table name 
    index_id: Index ID 
    index_name: Name of the index. 
    is_unique: Flag indicating whether an index has a unique index. 
    has_unique: Flag indicating whether the table has a unique index. 
    type_desc: Type of index; either clustered or non-clustered. 
    partition_number: Partition number. 
    fill_factor: Percentage of free space left on pages the index was created or rebuilt. 
    is_padded: Boolean value indicating whether fill factor is applied to nonleaf levels 
    reserved_page_count: Total number of pages reserved for the index. 
    size_in_mb: The amount of space in MB the index utilizes on disk. 
    buffered_page_count: Total number of pages in the buffer for the index. 
    buffer_mb: The amount of space in MB in the buffer for the index. 
    pct_in_buffer: The percentage of an index that is current in the SQL Server buffer. 
    table_buffer_mb: The amount of space in MB in the SQL Server buffer that is being 
        utilized by the table. 
    row_count: Number of rows in the index. 
    impact: Calculation of impact of a potential index.  This is based on the seeks and 
        scans that the index could have utilized multiplied by average improvement the index 
        would have provided.  This is included only for missing indexes. 
    existing_ranking: Ranking of the existing indexes ordered by user_total descending across
        the indexes for the table. 
    user_total: Total number of seek, scan, and lookup operations for the index. 
    user_total_pct: Percentage of total number of seek, scan, and lookup operations for this 
        index compared to all seek, scan, and lookup operations for existing indexes for the 
        table. 
    estimated_user_total_pct: Percentage of total number of seek, scan, and lookup operations 
        for this index compared to all seek, scan, and lookup operations for existing and 
        potential indexes for the table.  This number is naturally skewed because a seek for 
        potential Index A resulted in another operation on an existing index and both of 
        these operations would be counted. 
    user_seeks: Number of seek operations on the index. 
    user_scans: Number of scan operations on the index. 
    user_lookups: Number of lookup operations on the index. 
    user_updates: Number of update operations on the index. 
    read_to_update_ratio: Ratio of user_seeks, user_scans, and user_lookups to user_updates. 
    read_to_update: Division of user_seeks, user_scans, and user_lookups by user_updates. 
    update_to_read: Division of user_updates to user_seeks, user_scans by user_lookups. 
    row_lock_count: Cumulative number of row locks requested. 
    row_lock_wait_count: Cumulative number of times the Database Engine waited on a row lock. 
    row_lock_wait_in_ms: Total number of milliseconds the Database Engine waited on a row 
        lock. 
    row_block_pct: Percentage of row locks that encounter waits on a row lock. 
    avg_row_lock_waits_ms: Average number of milliseconds the Database Engine waited on a row 
        lock. 
    page_latch_wait_count: Cumulative number of times the page latch waits occurred 
    avg_page_latch_wait_ms: Average number of milliseconds the Database Engine waited on a 
        page latch wait. 
    page_io_latch_wait_count: Cumulative number of times the page IO latch waits occurred 
    avg_page_io_latch_wait_ms: Average number of milliseconds the Database Engine waited on a 
        page IO latch wait. 
    tree_page_latch_wait_count: Cumulative number of times the tree page latch waits occurred 
    avg_tree_page_latch_wait_ms: Average number of milliseconds the Database Engine waited on 
        a tree page latch wait. 
    tree_page_io_latch_wait_count: Cumulative number of times the tree page IO latch waits 
        occurred 
    avg_tree_page_io_latch_wait_ms: Average number of milliseconds the Database Engine waited 
        on a tree page IO latch wait. 
    read_operations: Cumulative count of range_scan_count and singleton_lookup_count 
        operations 
    leaf_writes: Cumulative count of leaf_insert_count, leaf_update_count, leaf_delete_count 
        and leaf_ghost_count operations 
    leaf_page_allocations: Cumulative count of leaf-level page allocations in the index or 
        heap.  For an index, a page allocation corresponds to a page split. 
    leaf_page_merges: Cumulative count of page merges at the leaf level. 
    nonleaf_writes: Cumulative count of leaf_insert_count, leaf_update_count and 
        leaf_delete_count operations 
    nonleaf_page_allocations: Cumulative count of page allocations caused by page splits 
        above the leaf level. 
    nonleaf_page_merges: Cumulative count of page merges above the leaf level. 
    indexed_columns: Columns that are part of the index, missing index or foreign key. 
    included_columns: Columns that are included in the index or missing index. 
    indexed_columns_compare: Column IDs that are part of the index, missing index or foreign 
        key 
    included_columns_compare: Column IDs that are included in the index or missing index. 
    duplicate_indexes: List of Indexes that exist on the table that are identical to the 
        index on this row. 
    overlapping_indexes: List of Indexes that exist on the table that overlap the index on 
        this row. 
    related_foreign_keys: List of foreign keys that are related to the index either as an 
        exact match or covering index. 
    related_foreign_keys_xml: XML document listing foreign keys that are related to the index 
        either as an exact match or covering index. 
		
*********************************************************************************************/ 
CREATE PROCEDURE dbo.sp_IndexAnalysis2005
    (
    @ObjectName sysname = NULL
    )
AS

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
SET NOCOUNT ON

DECLARE @SQL nvarchar(max)
    ,@DB_ID int
    ,@ObjectID int
    ,@DatabaseName nvarchar(max)
    ,@DefaultFillFactor tinyint
    ,@DefaultFileGroup nvarchar(max)

BEGIN TRY
    SELECT @DB_ID = DB_ID()
        ,@ObjectID = OBJECT_ID(DB_NAME(DB_ID()) + '.' + @ObjectName)
        ,@DatabaseName = QUOTENAME(DB_NAME(DB_ID()))

    -- Obtain Default Fill Factor
    SET @SQL = 'SELECT @DefaultFillFactor = CAST(value AS tinyint) '
        + 'FROM '+@DatabaseName+'.sys.configurations WHERE configuration_id = 109'

    EXEC sp_ExecuteSQL @SQL, N'@DefaultFillFactor tinyint OUTPUT', @DefaultFillFactor = @DefaultFillFactor OUTPUT

    -- Obtain Default File Group
    SET @SQL = 'SELECT @DefaultFileGroup = name '
        + 'FROM '+@DatabaseName+'.sys.data_spaces WHERE is_default = 1'

    EXEC sp_ExecuteSQL @SQL, N'@DefaultFileGroup sysname OUTPUT', @DefaultFileGroup = @DefaultFileGroup OUTPUT

    -- Obtain memory buffer information on database objects
    IF OBJECT_ID('tempdb..#MemoryBuffer') IS NOT NULL
        DROP TABLE #MemoryBuffer

    CREATE TABLE #MemoryBuffer 
        (
        object_id int
        ,index_id int
        ,partition_number int
        ,buffered_page_count int
        ,buffer_mb decimal(12, 2)
        )

    SET @SQL = 'WITH AllocationUnits
    AS (
        SELECT p.object_id
            ,p.index_id
            ,p.partition_number 
            ,au.allocation_unit_id
        FROM '+@DatabaseName+'.sys.allocation_units AS au
            INNER JOIN '+@DatabaseName+'.sys.partitions AS p ON au.container_id = p.hobt_id AND (au.type = 1 OR au.type = 3)
        UNION ALL
        SELECT p.object_id
            ,p.index_id
            ,p.partition_number 
            ,au.allocation_unit_id
        FROM '+@DatabaseName+'.sys.allocation_units AS au
            INNER JOIN '+@DatabaseName+'.sys.partitions AS p ON au.container_id = p.partition_id AND au.type = 2
    )
    SELECT au.object_id
        ,au.index_id
        ,au.partition_number
        ,COUNT(*)AS buffered_page_count
        ,CONVERT(decimal(12,2), CAST(COUNT(*) as bigint)*CAST(8 as float)/1024) as buffer_mb
    FROM '+@DatabaseName+'.sys.dm_os_buffer_descriptors AS bd 
        INNER JOIN AllocationUnits au ON bd.allocation_unit_id = au.allocation_unit_id
    WHERE bd.database_id = db_id()
    GROUP BY au.object_id, au.index_id, au.partition_number'

    INSERT INTO #MemoryBuffer
    EXEC sp_ExecuteSQL @SQL

    -- Create Main Temporary Tables
    IF OBJECT_ID('tempdb..#IndexBaseLine') IS NOT NULL
        DROP TABLE #IndexBaseLine

    CREATE TABLE #IndexBaseLine
        (
        row_id int IDENTITY(1,1)
        ,index_action varchar(10)
        ,pros varchar(25)
        ,cons varchar(25)
        ,filegroup nvarchar(128)
        ,schema_id int
        ,schema_name sysname
        ,object_id int
        ,table_name sysname
        ,index_id int
        ,index_name nvarchar(128)
        ,is_primary_key bit DEFAULT(0)
        ,is_unique bit DEFAULT(0)
        ,has_unique bit DEFAULT(0)
        ,type_desc nvarchar(67)
        ,partition_number int
        ,fill_factor tinyint
        ,is_padded bit
        ,reserved_page_count bigint
        ,size_in_mb decimal(12, 2)
        ,buffered_page_count int
        ,buffer_mb decimal(12, 2)
        ,pct_in_buffer decimal(12, 2)
        ,table_buffer_mb decimal(12, 2)
        ,row_count bigint
        ,impact int
        ,existing_ranking bigint
        ,user_total bigint
        ,user_total_pct decimal(6, 2)
        ,estimated_user_total_pct decimal(6, 2)
        ,user_seeks bigint
        ,user_scans bigint
        ,user_lookups bigint
        ,user_updates bigint
        ,read_to_update_ratio nvarchar(30)
        ,read_to_update int
        ,update_to_read int
        ,row_lock_count bigint
        ,row_lock_wait_count bigint
        ,row_lock_wait_in_ms bigint
        ,row_block_pct decimal(6, 2)
        ,avg_row_lock_waits_ms bigint
        ,page_latch_wait_count bigint
        ,avg_page_latch_wait_ms bigint
        ,page_io_latch_wait_count bigint
        ,avg_page_io_latch_wait_ms bigint
        ,tree_page_latch_wait_count bigint
        ,avg_tree_page_latch_wait_ms bigint
        ,tree_page_io_latch_wait_count bigint
        ,avg_tree_page_io_latch_wait_ms bigint    
        ,read_operations bigint
        ,leaf_writes bigint
        ,leaf_page_allocations bigint    
        ,leaf_page_merges bigint    
        ,nonleaf_writes bigint
        ,nonleaf_page_allocations bigint
        ,nonleaf_page_merges bigint    
        ,indexed_columns nvarchar(max)
        ,included_columns nvarchar(max)
        ,indexed_columns_compare nvarchar(max)
        ,included_columns_compare nvarchar(max)
        ,duplicate_indexes nvarchar(max)
        ,overlapping_indexes nvarchar(max)
        ,related_foreign_keys nvarchar(max)
        ,related_foreign_keys_xml xml
        )
     
     -- Populate stats on existing indexes.
     SET @SQL = N'SELECT 
        filegroup = ds.name
        , schema_id =  s.schema_id
        , schema_name = s.name
        , object_id = t.object_id
        , table_name = t.name
        , index_id = i.index_id
        , index_name = COALESCE(i.name, ''N/A'')
        , is_primary_key = i.is_primary_key
        , is_unique = i.is_unique
        , type_desc = CASE WHEN i.is_unique = 1 THEN ''UNIQUE '' ELSE '''' END + i.type_desc
        , partition_number = ps.partition_number
        , fill_factor = i.fill_factor
        , is_padded = i.is_padded
        , reserved_page_count = ps.reserved_page_count
        , size_in_mb = CAST(reserved_page_count * CAST(8 as float) / 1024 as decimal(12,2)) 
        , buffered_page_count = mb.buffered_page_count
        , buffer_mb = mb.buffer_mb
        , pct_in_buffer = CAST(100*buffer_mb/NULLIF(CAST(reserved_page_count * CAST(8 as float) / 1024 as decimal(12,2)),0) AS decimal(12,2)) 
        , row_count = row_count
        , existing_ranking = ROW_NUMBER() 
            OVER (PARTITION BY i.object_id ORDER BY i.is_primary_key desc, ius.user_seeks + ius.user_scans + ius.user_lookups desc) 
        , user_total = ius.user_seeks + ius.user_scans + ius.user_lookups
        , user_total_pct = COALESCE(CAST(100 * (ius.user_seeks + ius.user_scans + ius.user_lookups)
            /(NULLIF(SUM(ius.user_seeks + ius.user_scans + ius.user_lookups) 
            OVER(PARTITION BY i.object_id), 0) * 1.) as decimal(6,2)),0)
        , user_seeks = ius.user_seeks
        , user_scans = ius.user_scans
        , user_lookups = ius.user_lookups
        , user_updates = ius.user_updates
        , read_to_update_ratio = (1.*(ius.user_seeks + ius.user_scans + ius.user_lookups))/NULLIF(ius.user_updates,0)
        , read_to_update = CASE WHEN ius.user_seeks + ius.user_scans + ius.user_lookups >= ius.user_updates
            THEN CEILING(1.*(ius.user_seeks + ius.user_scans + ius.user_lookups)/COALESCE(NULLIF(ius.user_seeks,0),1)) 
            ELSE 0 END 
        , update_to_read = CASE WHEN ius.user_seeks + ius.user_scans + ius.user_lookups <= ius.user_updates
            THEN CEILING(1.*(ius.user_updates)/COALESCE(NULLIF(ius.user_seeks + ius.user_scans + ius.user_lookups,0),1)) 
            ELSE 0 END
        , row_lock_count = ios.row_lock_count
        , row_lock_wait_count = ios.row_lock_wait_count
        , row_lock_wait_in_ms = ios.row_lock_wait_in_ms
        , row_block_pct = CAST(100.0 * ios.row_lock_wait_count/NULLIF(ios.row_lock_count, 0) AS decimal(12,2)) 
        , avg_row_lock_waits_ms = CAST(1. * ios.row_lock_wait_in_ms /NULLIF(ios.row_lock_wait_count, 0) AS decimal(12,2))
        , page_latch_wait_count = ios.page_latch_wait_count
        , avg_page_latch_wait_ms = CAST(1. * page_latch_wait_in_ms / NULLIF(ios.page_io_latch_wait_count,0) AS decimal(12,2)) 
        , page_io_latch_wait_count = ios.page_io_latch_wait_count
        , avg_page_io_latch_wait_ms = CAST(1. * ios.page_io_latch_wait_in_ms / NULLIF(ios.page_io_latch_wait_count,0) AS decimal(12,2))
        , tree_page_latch_wait_count = NULL --ios.tree_page_latch_wait_count
        , avg_tree_page_latch_wait_ms = NULL --CAST(1. * tree_page_latch_wait_in_ms / NULLIF(ios.tree_page_io_latch_wait_count,0) AS decimal(12,2)) 
        , tree_page_io_latch_wait_count = NULL --ios.tree_page_io_latch_wait_count
        , avg_tree_page_io_latch_wait_ms = NULL --CAST(1. * ios.tree_page_io_latch_wait_in_ms / NULLIF(ios.tree_page_io_latch_wait_count,0) AS decimal(12,2)) 
        , read_operations = range_scan_count + singleton_lookup_count
        , leaf_writes = ios.leaf_insert_count + ios.leaf_update_count + ios.leaf_delete_count + ios.leaf_ghost_count
        , leaf_page_allocations = leaf_allocation_count
        , leaf_page_merges = ios.leaf_page_merge_count
        , nonleaf_writes = ios.nonleaf_insert_count + ios.nonleaf_update_count + ios.nonleaf_delete_count
        , nonleaf_page_allocations = ios.nonleaf_allocation_count
        , nonleaf_page_merges = ios.nonleaf_page_merge_count' +
    '    , indexed_columns = STUFF((
                SELECT '', '' + QUOTENAME(c.name)
                FROM '+@DatabaseName+'.sys.index_columns ic
                    INNER JOIN '+@DatabaseName+'.sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                WHERE i.object_id = ic.object_id
                AND i.index_id = ic.index_id
                AND is_included_column = 0
                ORDER BY key_ordinal ASC
                FOR XML PATH('''')), 1, 2, '''')
        , included_columns = STUFF((
                SELECT '', '' + QUOTENAME(c.name)
                FROM '+@DatabaseName+'.sys.index_columns ic
                    INNER JOIN '+@DatabaseName+'.sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                WHERE i.object_id = ic.object_id
                AND i.index_id = ic.index_id
                AND is_included_column = 1
                ORDER BY key_ordinal ASC
                FOR XML PATH('''')), 1, 2, '''') 
        , indexed_columns_compare = (SELECT QUOTENAME(ic.column_id,''('')
                FROM '+@DatabaseName+'.sys.index_columns ic
                WHERE i.object_id = ic.object_id
                AND i.index_id = ic.index_id
                AND is_included_column = 0
                ORDER BY key_ordinal ASC
                FOR XML PATH(''''))
        , included_columns_compare = COALESCE((
                SELECT QUOTENAME(ic.column_id, ''('')
                FROM '+@DatabaseName+'.sys.index_columns ic
                WHERE i.object_id = ic.object_id
                AND i.index_id = ic.index_id
                AND is_included_column = 1
                ORDER BY key_ordinal ASC
                FOR XML PATH('''')), SPACE(0)) 
    FROM '+@DatabaseName+'.sys.tables t
        INNER JOIN '+@DatabaseName+'.sys.schemas s ON t.schema_id = s.schema_id
        INNER JOIN '+@DatabaseName+'.sys.indexes i ON t.object_id = i.object_id
        INNER JOIN '+@DatabaseName+'.sys.data_spaces ds ON i.data_space_id = ds.data_space_id
        INNER JOIN '+@DatabaseName+'.sys.dm_db_partition_stats ps ON i.object_id = ps.object_id AND i.index_id = ps.index_id
        LEFT OUTER JOIN sys.dm_db_index_usage_stats ius ON i.object_id = ius.object_id AND i.index_id = ius.index_id AND ius.database_id = db_id()
        LEFT OUTER JOIN sys.dm_db_index_operational_stats(@DB_ID, NULL, NULL, NULL) ios ON ps.object_id = ios.object_id AND ps.index_id = ios.index_id AND ps.partition_number = ios.partition_number
        LEFT OUTER JOIN #MemoryBuffer mb ON ps.object_id = mb.object_id AND ps.index_id = mb.index_id AND ps.partition_number = mb.partition_number'

    IF @ObjectID IS NOT NULL
        SET @SQL = @SQL + CHAR(13) + 'WHERE t.object_id = @ObjectID '

    INSERT INTO #IndexBaseLine
        (
        filegroup, schema_id, schema_name, object_id, table_name, index_id, index_name, is_primary_key, is_unique, type_desc, partition_number, fill_factor
        , is_padded, reserved_page_count, size_in_mb, buffered_page_count, buffer_mb, pct_in_buffer, row_count, existing_ranking, user_total
        , user_total_pct, user_seeks, user_scans, user_lookups, user_updates, read_to_update_ratio, read_to_update, update_to_read, row_lock_count
        , row_lock_wait_count, row_lock_wait_in_ms, row_block_pct, avg_row_lock_waits_ms, page_latch_wait_count, avg_page_latch_wait_ms
        , page_io_latch_wait_count, avg_page_io_latch_wait_ms, tree_page_latch_wait_count, avg_tree_page_latch_wait_ms, tree_page_io_latch_wait_count
        , avg_tree_page_io_latch_wait_ms, read_operations, leaf_writes, leaf_page_allocations, leaf_page_merges, nonleaf_writes
        , nonleaf_page_allocations, nonleaf_page_merges, indexed_columns, included_columns, indexed_columns_compare, included_columns_compare
        )   
    EXEC sp_ExecuteSQL @SQL, N'@DB_ID int, @ObjectID int', @DB_ID = @DB_ID, @ObjectID = @ObjectID

    -- Populate stats on missing indexes.
     SET @SQL = N'SELECT s.schema_id
        ,s.name AS schema_name
        ,t.object_id
        ,t.name AS table_name
        ,''--MISSING--'' AS index_name
        ,''--NONCLUSTERED--'' AS type_desc
        ,(migs.user_seeks + migs.user_scans) * migs.avg_user_impact as impact
        ,0 AS existing_ranking
        ,migs.user_seeks + migs.user_scans as user_total
        ,migs.user_seeks 
        ,migs.user_scans
        ,0 as user_lookups
        ,COALESCE(equality_columns + CASE WHEN inequality_columns IS NOT NULL THEN '', '' ELSE SPACE(0) END, SPACE(0)) + COALESCE(inequality_columns, SPACE(0)) as indexed_columns
        ,included_columns
    FROM '+@DatabaseName+'.sys.tables t
        INNER JOIN '+@DatabaseName+'.sys.schemas s ON t.schema_id = s.schema_id
        INNER JOIN sys.dm_db_missing_index_details mid ON t.object_id = mid.object_id
        INNER JOIN sys.dm_db_missing_index_groups mig ON mid.index_handle = mig.index_handle
        INNER JOIN sys.dm_db_missing_index_group_stats migs ON mig.index_group_handle = migs.group_handle
    WHERE mid.database_id = @DB_ID'

    IF @ObjectID IS NOT NULL
        SET @SQL = @SQL + CHAR(13) + 'AND t.object_id = @ObjectID '

    INSERT INTO #IndexBaseLine
        (schema_id, schema_name, object_id, table_name, index_name, type_desc, impact, existing_ranking, user_total, user_seeks, user_scans, user_lookups, indexed_columns, included_columns)
    EXEC sp_ExecuteSQL @SQL, N'@DB_ID int, @ObjectID int', @DB_ID = @DB_ID, @ObjectID = @ObjectID

    -- Collect foreign key information.
    IF OBJECT_ID('tempdb..#ForeignKeys') IS NOT NULL
        DROP TABLE #ForeignKeys

    CREATE TABLE #ForeignKeys
        (
        foreign_key_name sysname
        ,object_id int
        ,fk_columns nvarchar(max)
        ,fk_columns_compare nvarchar(max)
        )
		
     SET @SQL = N'SELECT fk.name + ''|PARENT'' AS foreign_key_name
        ,fkc.parent_object_id AS object_id
        ,STUFF((SELECT '', '' + QUOTENAME(c.name)
            FROM '+@DatabaseName+'.sys.foreign_key_columns ifkc
                INNER JOIN '+@DatabaseName+'.sys.columns c ON ifkc.parent_object_id = c.object_id AND ifkc.parent_column_id = c.column_id
            WHERE fk.object_id = ifkc.constraint_object_id
            ORDER BY ifkc.constraint_column_id
            FOR XML PATH('''')), 1, 2, '''') AS fk_columns
        ,(SELECT QUOTENAME(ifkc.parent_column_id,''('')
            FROM '+@DatabaseName+'.sys.foreign_key_columns ifkc
            WHERE fk.object_id = ifkc.constraint_object_id
            ORDER BY ifkc.constraint_column_id
            FOR XML PATH('''')) AS fk_columns_compare
    FROM '+@DatabaseName+'.sys.foreign_keys fk
        INNER JOIN '+@DatabaseName+'.sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
    WHERE fkc.constraint_column_id = 1
    AND (fkc.parent_object_id = @ObjectID OR @ObjectID IS NULL)
    UNION ALL
    SELECT fk.name + ''|REFERENCED'' as foreign_key_name
        ,fkc.referenced_object_id AS object_id
        ,STUFF((SELECT '', '' + QUOTENAME(c.name)
            FROM '+@DatabaseName+'.sys.foreign_key_columns ifkc
                INNER JOIN '+@DatabaseName+'.sys.columns c ON ifkc.referenced_object_id = c.object_id AND ifkc.referenced_column_id = c.column_id
            WHERE fk.object_id = ifkc.constraint_object_id
            ORDER BY ifkc.constraint_column_id
            FOR XML PATH('''')), 1, 2, '''') AS fk_columns
        ,(SELECT QUOTENAME(ifkc.referenced_column_id,''('')
            FROM '+@DatabaseName+'.sys.foreign_key_columns ifkc
            WHERE fk.object_id = ifkc.constraint_object_id
            ORDER BY ifkc.constraint_column_id
            FOR XML PATH('''')) AS fk_columns_compare
    FROM '+@DatabaseName+'.sys.foreign_keys fk
        INNER JOIN '+@DatabaseName+'.sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
    WHERE fkc.constraint_column_id = 1
    AND (fkc.referenced_object_id = @ObjectID OR @ObjectID IS NULL)'

    INSERT INTO #ForeignKeys
        (foreign_key_name, object_id, fk_columns, fk_columns_compare)
    EXEC sp_ExecuteSQL @SQL, N'@DB_ID int, @ObjectID int', @DB_ID = @DB_ID, @ObjectID = @ObjectID

    -- Determine duplicate, overlapping, and foreign key index information
    UPDATE ibl
    SET duplicate_indexes = STUFF((SELECT ', ' + index_name AS [data()]
            FROM #IndexBaseLine iibl
            WHERE ibl.object_id = iibl.object_id
            AND ibl.index_id > iibl.index_id
            AND ibl.indexed_columns_compare = iibl.indexed_columns_compare
            AND ibl.included_columns_compare = iibl.included_columns_compare
            FOR XML PATH('')), 1, 2, '')
        ,overlapping_indexes = STUFF((SELECT ', ' + index_name AS [data()]
            FROM #IndexBaseLine iibl
            WHERE ibl.object_id = iibl.object_id
            AND ibl.index_id <> iibl.index_id
            AND (ibl.indexed_columns_compare LIKE iibl.indexed_columns_compare + '%' 
                OR iibl.indexed_columns_compare LIKE ibl.indexed_columns_compare + '%')
            AND ibl.indexed_columns_compare <> iibl.indexed_columns_compare 
            FOR XML PATH('')), 1, 2, '')
        ,related_foreign_keys = STUFF((SELECT ', ' + foreign_key_name AS [data()]
            FROM #ForeignKeys ifk
            WHERE ifk.object_id = ibl.object_id
            AND ibl.indexed_columns_compare LIKE ifk.fk_columns_compare + '%'
            FOR XML PATH('')), 1, 2, '')
        ,related_foreign_keys_xml = CAST((SELECT foreign_key_name
            FROM #ForeignKeys ForeignKeys
            WHERE ForeignKeys.object_id = ibl.object_id
            AND ibl.indexed_columns_compare LIKE ForeignKeys.fk_columns_compare + '%'
            FOR XML AUTO) as xml)  
    FROM #IndexBaseLine ibl


     -- Populate stats on missing foreign key indexes
    SET @SQL = N'SELECT s.schema_id
        ,s.name AS schema_name
        ,t.object_id
        ,t.name AS table_name
        ,fk.foreign_key_name AS index_name
        ,''--MISSING FOREIGN KEY--'' as type_desc
        ,9999
        ,fk.fk_columns
        ,t.name AS related_foreign_keys
    FROM '+@DatabaseName+'.sys.tables t
        INNER JOIN '+@DatabaseName+'.sys.schemas s ON t.schema_id = s.schema_id
        INNER JOIN #ForeignKeys fk ON t.object_id = fk.object_id
        LEFT OUTER JOIN #IndexBaseLine ia ON fk.object_id = ia.object_id AND ia.indexed_columns_compare LIKE fk.fk_columns_compare + ''%''
    WHERE ia.index_name IS NULL'

    INSERT INTO #IndexBaseLine
        (schema_id, schema_name, object_id, table_name, index_name, type_desc, existing_ranking, indexed_columns, related_foreign_keys)
    EXEC sp_ExecuteSQL @SQL, N'@DB_ID int, @ObjectID int', @DB_ID = @DB_ID, @ObjectID = @ObjectID

    -- Determine whether tables have unique indexes
    SET @SQL = N'UPDATE ibl
    SET has_unique = 1
    FROM #IndexBaseLine ibl
        INNER JOIN (SELECT DISTINCT object_id FROM '+@DatabaseName+'.sys.indexes i WHERE i.is_unique = 1) x ON ibl.object_id = x.object_id'
        
    EXEC sp_ExecuteSQL @SQL

    -- Calculate estimated user total for each index.
    ;WITH Aggregation
    AS (
        SELECT row_id
            ,CAST(100. * (user_seeks + user_scans + user_lookups)
                /(NULLIF(SUM(user_seeks + user_scans + user_lookups) 
                OVER(PARTITION BY schema_name, table_name), 0) * 1.) as decimal(12,2)) AS estimated_user_total_pct
            ,SUM(buffer_mb) OVER(PARTITION BY schema_name, table_name) as table_buffer_mb
        FROM #IndexBaseLine 
    )
    UPDATE ibl
    SET estimated_user_total_pct = COALESCE(a.estimated_user_total_pct, 0)
        ,table_buffer_mb = a.table_buffer_mb
    FROM #IndexBaseLine ibl
        INNER JOIN Aggregation a ON ibl.row_id = a.row_id

    -- Update Index Action information
    ;WITH IndexAction
    AS (
        SELECT row_id
            ,CASE WHEN user_lookups > user_seeks AND type_desc IN ('CLUSTERED', 'HEAP', 'UNIQUE CLUSTERED') THEN 'REALIGN'
                WHEN duplicate_indexes IS NOT NULL THEN 'DROP' 
                WHEN type_desc = '--MISSING FOREIGN KEY--' THEN 'CREATE'
                WHEN type_desc = 'XML' THEN '---'
                WHEN is_unique = 1 THEN '---'
                WHEN related_foreign_keys IS NOT NULL THEN '---'
                WHEN type_desc = '--NONCLUSTERED--' AND ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY user_total desc) <= 10 AND estimated_user_total_pct > 1 THEN 'CREATE'
                WHEN type_desc = '--NONCLUSTERED--' THEN 'BLEND'
                WHEN ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY user_total desc, existing_ranking) > 10 THEN 'DROP' 
                WHEN user_total = 0 THEN 'DROP' 
                ELSE '---' END AS index_action
        FROM #IndexBaseLine
    )
    UPDATE ibl
    SET index_action = ia.index_action
    FROM #IndexBaseLine ibl INNER JOIN IndexAction ia
    ON ibl.row_id = ia.row_id

    -- Update Pro/Con statuses
    UPDATE #IndexBaseLine
    SET Pros = COALESCE(STUFF(CASE WHEN related_foreign_keys IS NOT NULL THEN ', FK' ELSE '' END
            + CASE WHEN is_unique = 1 THEN ', UQ' ELSE '' END
            + COALESCE(', ' + CASE WHEN read_to_update BETWEEN 1 AND 9 THEN '$'
                WHEN read_to_update BETWEEN 10 AND 99 THEN '$$'
                WHEN read_to_update BETWEEN 100 AND 999 THEN '$$$'
                WHEN read_to_update > 999 THEN '$$$+' END, '')
            ,1,2,''),'')
        ,Cons = COALESCE(STUFF(CASE WHEN user_seeks / NULLIF(user_scans,0) < 1000 THEN ', SCN' ELSE '' END
            + CASE WHEN duplicate_indexes IS NOT NULL THEN ', DP' ELSE '' END
            + CASE WHEN overlapping_indexes IS NOT NULL THEN ', OV' ELSE '' END
            + COALESCE(', ' + CASE WHEN update_to_read BETWEEN 1 AND 9 THEN '$'
                WHEN update_to_read BETWEEN 10 AND 99 THEN '$$'
                WHEN update_to_read BETWEEN 100 AND 999 THEN '$$$'
                WHEN update_to_read > 999 THEN '$$$+' END, '')
            ,1,2,''),'')

    --Final Output
    SELECT
        index_action
        , pros
        , cons
        , QUOTENAME(schema_name) + '.' + QUOTENAME(table_name) as object_name
        , index_name
        , type_desc
        , indexed_columns
        , included_columns
        , is_primary_key
        , is_unique
        , has_unique
        , partition_number
        , fill_factor
        , is_padded
        , size_in_mb
        , buffer_mb
        , table_buffer_mb
        , pct_in_buffer
        , row_count
        , user_total_pct
        , estimated_user_total_pct
        , impact
        , user_total
        , user_seeks
        , user_scans
        , user_lookups
        , user_updates
        , read_to_update_ratio
        , read_to_update
        , update_to_read
        , row_lock_count
        , row_lock_wait_count
        , row_lock_wait_in_ms
        , row_block_pct
        , avg_row_lock_waits_ms
        , page_latch_wait_count
        , avg_page_latch_wait_ms
        , page_io_latch_wait_count
        , avg_page_io_latch_wait_ms
        , tree_page_latch_wait_count
        , avg_tree_page_latch_wait_ms
        , tree_page_io_latch_wait_count
        , avg_tree_page_io_latch_wait_ms
        , read_operations
        , leaf_writes
        , leaf_page_allocations
        , leaf_page_merges
        , nonleaf_writes
        , nonleaf_page_allocations
        , nonleaf_page_merges
        , duplicate_indexes
        , overlapping_indexes
        , related_foreign_keys
        , related_foreign_keys_xml
        ,CAST('<?query --' + CHAR(13)
            + CASE WHEN is_primary_key = 1 OR is_unique = 1 THEN  '-- !! WARNING !! Drop statement will fail if there are dependent objects.' + CHAR(13) + CHAR(13) ELSE SPACE(0) END
            + CASE WHEN index_id = 0 THEN NULL
                WHEN is_primary_key = 1 THEN
                    'IF  EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N''' + QUOTENAME(schema_name) + '.' + QUOTENAME(table_name) + ''') AND name = N''' + index_name + ''')' + CHAR(13)
                    + '    ALTER TABLE '+QUOTENAME(schema_name) + '.' + QUOTENAME(table_name)+' DROP CONSTRAINT ' + QUOTENAME(index_name) + CHAR(13)
                ELSE 
                    'IF  EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N''' + QUOTENAME(schema_name) + '.' + QUOTENAME(table_name) + ''') AND name = N''' 
                    + CASE WHEN index_id IS NULL THEN '<index_name, sysname, ind_test>' ELSE index_name END + ''')' + CHAR(13)
                    + '    DROP INDEX ' + CASE WHEN index_id IS NULL THEN '<index_name, sysname, ind_test>' ELSE index_name END 
                    + ' ON ' + QUOTENAME(schema_name) + '.' + QUOTENAME(table_name) + CHAR(13)
                END
            + CHAR(13) + '--?>' AS xml) as ddl_drop
       ,CAST('<?query --' + CHAR(13)
            + CASE WHEN index_id = 0 THEN NULL
                WHEN is_primary_key = 1 THEN
                    'ALTER TABLE '+QUOTENAME(schema_name) + '.' + QUOTENAME(table_name)+' ADD  CONSTRAINT ' + QUOTENAME(index_name) + ' PRIMARY KEY ' 
                    + CASE WHEN index_id = 1 THEN 'CLUSTERED ' ELSE 'NONCLUSTERED ' END + CHAR(13)
                    + '     ('+indexed_columns+')' + CHAR(13)
                    + CASE WHEN included_columns IS NOT NULL THEN ' INCLUDE ('+included_columns+')' ELSE '' END
                ELSE 'CREATE ' 
                    + CASE WHEN is_unique = 1 THEN 'UNIQUE ' ELSE '' END
                    + CASE WHEN index_id = 1 THEN 'CLUSTERED ' ELSE 'NONCLUSTERED ' END 
                    + 'INDEX ' + CASE WHEN index_id IS NULL THEN '<index_name, sysname, ind_test>' ELSE index_name END
                    + ' ON ' + QUOTENAME(schema_name) + '.' + QUOTENAME(table_name) + CHAR(13)
                    + '     ('+indexed_columns+')' + CHAR(13)
                    + CASE WHEN included_columns IS NOT NULL THEN ' INCLUDE ('+included_columns+')' ELSE '' END
                END
            + ' WITH (PAD_INDEX  = ' + CASE WHEN is_padded = 1 THEN 'ON' ELSE 'OFF' END
            + ', STATISTICS_NORECOMPUTE  = OFF'
            + ', SORT_IN_TEMPDB = OFF'
            + ', IGNORE_DUP_KEY = OFF'
            + ', ONLINE = OFF'
            + ', ALLOW_ROW_LOCKS  = ON'
            + ', ALLOW_PAGE_LOCKS  = ON'
            + ', FILLFACTOR = ' 
            + CONVERT(varchar(3), CASE WHEN COALESCE(fill_factor, @DefaultFillFactor) = 0 THEN 100 ELSE COALESCE(fill_factor, @DefaultFillFactor) END)       
            + ') ON ' + QUOTENAME(COALESCE(filegroup, @DefaultFileGroup))
            + CHAR(13) + '--?>' AS xml) as ddl_create
    FROM #IndexBaseLine
    WHERE (estimated_user_total_pct > 0.01 AND index_id IS NULL)
    OR related_foreign_keys IS NOT NULL
    OR index_id IS NOT NULL
    ORDER BY table_buffer_mb DESC, object_id, user_total DESC
    
END TRY
BEGIN CATCH
    DECLARE @ERROR_MESSAGE nvarchar(2048)
        ,@ERROR_SEVERITY int
        ,@ERROR_STATE INT
        
    SELECT @ERROR_MESSAGE  = ERROR_MESSAGE()
        ,@ERROR_SEVERITY = ERROR_SEVERITY()
        ,@ERROR_STATE = ERROR_STATE()
    
    RAISERROR(@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE)
END CATCH
GO