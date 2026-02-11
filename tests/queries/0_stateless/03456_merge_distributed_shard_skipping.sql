-- Test that merge() table function properly skips unused shards on distributed tables

DROP TABLE IF EXISTS test_local;
DROP TABLE IF EXISTS test_distributed;
DROP DICTIONARY IF EXISTS region_dict;

-- Create dictionary for sharding key
CREATE DICTIONARY region_dict (
    region String,
    shard_id UInt64
) PRIMARY KEY region
SOURCE(CLICKHOUSE(QUERY 'SELECT region, shard_id FROM (VALUES (\'eu-west-1\', 1), (\'us-east-1\', 2))'))
LAYOUT(HASHED())
LIFETIME(MIN 0 MAX 0);

-- Create local table
CREATE TABLE test_local (
    EventDate Date,
    Region String,
    Value UInt64
) ENGINE = MergeTree()
PARTITION BY EventDate
ORDER BY (EventDate, Region);

-- Insert test data
INSERT INTO test_local VALUES ('2025-01-01', 'eu-west-1', 100), ('2025-01-01', 'us-east-1', 200);

-- Create distributed table with dictionary-based sharding key
CREATE TABLE test_distributed AS test_local
ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), test_local, dictGet('region_dict', 'shard_id', Region));

-- Test 1: Direct query on distributed table (baseline - should work)
SELECT count() FROM test_distributed WHERE Region = 'eu-west-1' AND EventDate = '2025-01-01';

-- Test 2: Query through merge() with new analyzer (this was failing before the fix)
SELECT count() FROM merge(currentDatabase(), '^test_distributed')
WHERE Region = 'eu-west-1' AND EventDate = '2025-01-01';

-- Test 3: Query through merge() with legacy analyzer (should continue to work)
SELECT count() FROM merge(currentDatabase(), '^test_distributed')
WHERE Region = 'eu-west-1' AND EventDate = '2025-01-01'
SETTINGS allow_experimental_analyzer = 0;

-- Test 4: With force_optimize_skip_unused_shards enabled (should not throw error)
SELECT count() FROM merge(currentDatabase(), '^test_distributed')
WHERE Region = 'eu-west-1' AND EventDate = '2025-01-01'
SETTINGS force_optimize_skip_unused_shards = 1;

-- Cleanup
DROP TABLE test_distributed;
DROP TABLE test_local;
DROP DICTIONARY region_dict;
