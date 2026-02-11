-- Test merge() with distributed table using simple column as sharding key

DROP TABLE IF EXISTS simple_local;
DROP TABLE IF EXISTS simple_distributed;

CREATE TABLE simple_local (
    id UInt64,
    region String,
    value String
) ENGINE = MergeTree()
ORDER BY id;

INSERT INTO simple_local VALUES (1, 'eu', 'data1'), (2, 'us', 'data2');

-- Simple sharding key: just a hash of a column
CREATE TABLE simple_distributed AS simple_local
ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), simple_local, cityHash64(region));

-- Should work with merge()
SELECT count() FROM merge(currentDatabase(), '^simple_distributed') WHERE region = 'eu';

-- Should work with forced optimization
SELECT count() FROM merge(currentDatabase(), '^simple_distributed')
WHERE region = 'eu'
SETTINGS force_optimize_skip_unused_shards = 1;

DROP TABLE simple_distributed;
DROP TABLE simple_local;
