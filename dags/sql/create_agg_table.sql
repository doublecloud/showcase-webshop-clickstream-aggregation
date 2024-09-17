-- you can omit IF NOT EXISTS: the same check is done in Airflow,
-- but then it's tiny bit less convenient to copy-paste if needed
CREATE TABLE IF NOT EXISTS performance_aggregates
(
    ts_start DateTime,
    ts_end DateTime,
    visitors Integer,
    num_buyers Integer,
    num_purchases Integer,
    total_revenue Integer,
    average_revenue_per_purchase Float32
) ENGINE = MergeTree() PRIMARY KEY ts_start
