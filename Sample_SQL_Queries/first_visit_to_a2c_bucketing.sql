WITH a AS (
    SELECT 
        event_timestamp AS time_atc, 
        event_name,
        user_pseudo_id 
    FROM 
        sample_table
    WHERE 
        event_name = 'add_to_cart' 
        AND event_date BETWEEN '20220101' AND '20220701'
    ORDER BY 
        1 ASC
),
b AS (
    SELECT 
        event_timestamp AS time_v, 
        event_name,
        user_pseudo_id 
    FROM 
        sample_table
    WHERE 
        event_name = 'first_visit' 
        AND event_date BETWEEN '20220101' AND '20220701'
    ORDER BY 
        1 ASC
),
c AS (
    SELECT 
        a.user_pseudo_id, 
        b.event_name, 
        time_v, 
        a.event_name, 
        time_atc, 
        toDate(time_atc) - toDate(time_v) AS days,
        toUInt32(toDateTime(time_atc) - toDateTime(time_v)) / 3600 / 24 AS days_1,
        toUInt32(toDateTime(time_atc) - toDateTime(time_v)) % 3600 / 60 / 1440 AS days_2
    FROM 
        a 
    JOIN 
        b 
    ON 
        a.user_pseudo_id = b.user_pseudo_id
    WHERE 
        toDate(time_atc) > toDate(time_v)
),
d AS (
    SELECT 
        user_pseudo_id, 
        CASE 
            WHEN days + days_1 + days_2 >= 1 AND days + days_1 + days_2 <= 5 THEN '1-5'
            WHEN days + days_1 + days_2 > 5 AND days + days_1 + days_2 <= 15 THEN '5-15'
            WHEN days + days_1 + days_2 > 15 AND days + days_1 + days_2 <= 50 THEN '15-50'
            WHEN days + days_1 + days_2 > 50 THEN 'after 50 days'
        END AS days 
    FROM 
        c
)
SELECT 
    days, 
    COUNT(days) AS conversion_count 
FROM 
    d
GROUP BY 
    1
ORDER BY 
    days ASC;