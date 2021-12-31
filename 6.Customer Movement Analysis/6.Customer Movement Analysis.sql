WITH month_table AS (
    SELECT DISTINCT CUST_CODE, DATE_TRUNC(PARSE_DATE('%Y%m%d', CAST(SHOP_DATE AS STRING)), month) curr_month
    FROM `grand-plasma-331409.supermarket_data.supermarket`
    WHERE CUST_CODE IS NOT NULL
),
prev_table AS (
    SELECT CUST_CODE, curr_month, LAG(curr_month, 1) OVER (PARTITION BY CUST_CODE ORDER BY curr_month) AS prev_month
    FROM month_table
),
status_table AS (
    SELECT *
    ,CASE 
        WHEN DATE_DIFF(curr_month, prev_month, MONTH) IS NULL THEN 'New_User'
        WHEN DATE_DIFF(curr_month, prev_month, MONTH) = 1 THEN 'Repeat_User'
        WHEN DATE_DIFF(curr_month, prev_month, MONTH) > 1 THEN 'Reactivate_User'
    ELSE NULL END AS month_status
    FROM prev_table
),
churn_table AS (
    SELECT *
    FROM status_table
    UNION ALL
    SELECT CUST_CODE, DATE_ADD(curr_month, INTERVAL 1 MONTH) month_status, curr_month as prev_month, 'Churn_User' AS month_status
    FROM (
        SELECT CUST_CODE, curr_month
            ,LEAD(curr_month, 1) OVER (PARTITION BY CUST_CODE ORDER BY curr_month) AS next_transac
            ,DATE_DIFF(LEAD(curr_month, 1) OVER (PARTITION BY CUST_CODE ORDER BY curr_month), curr_month, MONTH) AS diff_next
        FROM status_table
    ) WHERE
        (diff_next > 1 or diff_next is null)
        AND curr_month < (SELECT MAX(curr_month) FROM month_table)
)
SELECT curr_month
    ,COUNT(DISTINCT CASE WHEN month_status = 'New_User' THEN CUST_CODE ELSE NULL END) AS new_user
    ,COUNT(DISTINCT CASE WHEN month_status = 'Repeat_User' THEN CUST_CODE ELSE NULL END) AS repeat_user
    ,COUNT(DISTINCT CASE WHEN month_status = 'Reactivate_User' THEN CUST_CODE ELSE NULL END) AS reactivate_user
    ,-COUNT(DISTINCT CASE WHEN month_status = 'Churn_User' THEN CUST_CODE ELSE NULL END) AS churn_user
    FROM churn_table
    GROUP BY curr_month
    ORDER BY curr_month 