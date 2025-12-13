-- Define a Common Table Expression (CTE) to calculate total returns per customer, per store, for the year 2000.
WITH customer_total_return AS (
    SELECT
        sr_customer_sk AS ctr_customer_sk,
        sr_store_sk AS ctr_store_sk,
        sum(sr_return_amt) AS ctr_total_return
    FROM
        ${database}.${schema}.store_returns,
        ${database}.${schema}.date_dim
    WHERE
        sr_returned_date_sk = d_date_sk
      AND d_year = 2000
    GROUP BY
        sr_customer_sk,
        sr_store_sk
)
-- Main query: Select customers from Tennessee (TN) who have returns significantly higher
-- than the average return for the specific store where they returned items.
SELECT
    c_customer_id
FROM
    customer_total_return ctr1,
    ${database}.${schema}.store,
    ${database}.${schema}.customer
WHERE
  -- Core business logic: Find customers whose total return is greater than 1.2 times the average
  -- return for that customer's store. This is a correlated subquery.
    ctr1.ctr_total_return > (
        SELECT
            avg(ctr_total_return) * 1.2
        FROM
            customer_total_return ctr2
        WHERE
            ctr1.ctr_store_sk = ctr2.ctr_store_sk
    )
  -- Join and filter conditions to link the returns to specific stores and customers.
  AND s_store_sk = ctr1.ctr_store_sk
  AND s_state = 'TN'
  AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY
    c_customer_id ASC
    LIMIT 100;