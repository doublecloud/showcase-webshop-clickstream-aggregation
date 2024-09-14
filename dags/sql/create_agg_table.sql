-- you can omit IF NOT EXISTS: the same check is done in Airflow,
-- but then it's tiny bit less convenient to copy-paste if needed
CREATE TABLE IF NOT EXISTS performance_aggregates
(
    ts_start DateTime,
    ts_end DateTime,
    total_revenue Integer,
    num_buyers Integer,
    visitors Integer,
    num_purchases Integer,
    average_revenue_per_purchase Float32
) ENGINE = MergeTree() PRIMARY KEY ts_start
