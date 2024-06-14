WITH a AS (
    SELECT 
        geo.region AS region,
        geo.country AS country,
        COUNT(event_name) AS total
    FROM 
        sample_table
    WHERE 
        event_name = 'add_to_cart'
    GROUP BY 
        geo.region, geo.country
    ORDER BY 
        total DESC
    LIMIT 10
),
b AS (
    SELECT 
        CASE 
            WHEN total < 80000 THEN 'others'
            ELSE region 
        END AS region,
        CASE 
            WHEN total > 80000 THEN 'others'
            ELSE region 
        END AS region_1,
        total 
    FROM 
        a
)
SELECT 
    region, 
    region_1, 
    total 
FROM 
    b
LIMIT 10;
