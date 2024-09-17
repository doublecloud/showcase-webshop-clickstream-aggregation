INSERT INTO webshop.performance_aggregates
  (ts_start, ts_end, visitors, num_buyers, num_purchases, total_revenue, average_revenue_per_purchase)
WITH good_data AS (
    SELECT * FROM webshop.events_webshop
    WHERE 1=1
        AND detectedDuplicate=false
        AND detectedCorruption=false
), visitors AS (
    SELECT
        toStartOfHour(my_ts::DateTime64) AS hour,
        COUNT(DISTINCT partyId) AS visitors
    FROM good_data
    GROUP BY hour
), performance AS (
    SELECT
        toStartOfHour(my_ts::DateTime64) AS hour,
        SUM(item_price) AS total_revenue,
        COUNT(DISTINCT partyId) AS num_buyers,
        COUNT(DISTINCT sessionId) AS num_purchases,
        round(total_revenue / num_purchases, 1) AS average_revenue_per_purchase
    FROM good_data
    WHERE eventType='itemBuyEvent'
    GROUP BY hour
)
SELECT
    v.hour as ts_start,
    v.hour + INTERVAL 1 hour as ts_end,
    v.visitors,
    p.num_buyers,
    p.num_purchases,
    p.total_revenue,
    p.average_revenue_per_purchase
FROM visitors v JOIN performance p ON v.hour = p.hour
