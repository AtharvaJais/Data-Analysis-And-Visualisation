WITH sample_table_temp AS (
    SELECT 
        sample_table.date_time,
        sample_table.iot_id,
        CASE
            WHEN sample_table.kw = ''::text THEN NULL::numeric
            WHEN kw = '2164.76.kvar=0.00' THEN NULL::numeric
            ELSE sample_table.kw::numeric
        END AS kw
    FROM 
        sample_table
    WHERE 
        sample_table.iot_id = ANY (ARRAY['MX02202330'::text, 'MX02202335'::text])
), temp_main AS (
    SELECT DISTINCT 
        sample_table_temp.date_time::date AS new_date
    FROM 
        sample_table_temp
), temp1 AS (
    SELECT 
        concat(temp_main.new_date, ' ', '00:00:00') AS date_time
    FROM 
        temp_main
    UNION ALL
    SELECT 
        concat(temp_main.new_date, ' ', '08:00:00') AS date_time
    FROM 
        temp_main
    UNION ALL
    SELECT 
        concat(temp_main.new_date, ' ', '20:00:00') AS date_time
    FROM 
        temp_main
), temp2 AS (
    SELECT 
        temp1.date_time,
        'MX02202330'::text AS iot_id,
        NULL::text AS kw
    FROM 
        temp1
    UNION ALL
    SELECT 
        temp1.date_time,
        'MX02202335'::text AS iot_id,
        NULL::text AS kw
    FROM 
        temp1
), sample_table_union AS (
    SELECT 
        temp2.date_time::timestamp without time zone AS date_time,
        temp2.iot_id,
        temp2.kw
    FROM 
        temp2
    UNION ALL
    SELECT 
        sample_table_temp.date_time::timestamp without time zone AS date_time,
        sample_table_temp.iot_id,
        sample_table_temp.kw::text AS kw
    FROM 
        sample_table_temp
    WHERE 
        NOT (concat(sample_table_temp.date_time, sample_table_temp.iot_id) IN (
            SELECT 
                concat(temp2.date_time, temp2.iot_id) AS concat
            FROM 
                temp2
            GROUP BY 
                concat(temp2.date_time, temp2.iot_id)
        ))
), sample_table_new AS (
    SELECT 
        sample_table_union.date_time,
        sample_table_union.iot_id,
        CASE
            WHEN sample_table_union.kw = ''::text THEN NULL::numeric
            WHEN sample_table_union.kw ~~ '%value%'::text THEN NULL::numeric
            WHEN sample_table_union.kw IS NULL THEN lag(sample_table_union.kw) OVER (PARTITION BY sample_table_union.iot_id ORDER BY sample_table_union.date_time)::numeric
            ELSE sample_table_union.kw::numeric
        END AS kw
    FROM 
        sample_table_union
), a AS (
    SELECT 
        sample_table_new.date_time,
        sample_table_new.iot_id,
        CASE
            WHEN sample_table_new.date_time::time without time zone >= '08:00:00'::time without time zone AND sample_table_new.date_time::time without time zone < '20:00:00'::time without time zone THEN 'shift_a'::text
            WHEN sample_table_new.date_time::time without time zone >= '20:00:00'::time without time zone AND sample_table_new.date_time::time without time zone <= '23:59:59'::time without time zone THEN 'shift_b1'::text
            WHEN sample_table_new.date_time::time without time zone >= '00:00:00'::time without time zone AND sample_table_new.date_time::time without time zone < '08:00:00'::time without time zone THEN 'shift_b2'::text
            ELSE NULL::text
        END AS shift,
        sample_table_new.kw,
        CASE
            WHEN sample_table_new.kw >= 30000::numeric THEN 'Up'::text
            WHEN sample_table_new.kw < 30000::numeric THEN 'Down'::text
            ELSE NULL::text
        END AS status
    FROM 
        sample_table_new
), b AS (
    SELECT 
        a.date_time,
        a.iot_id,
        a.shift,
        a.kw,
        a.status,
        lag(a.status) OVER (PARTITION BY a.iot_id ORDER BY a.date_time) AS lag_status
    FROM 
        a
), c AS (
    SELECT 
        b.date_time,
        b.iot_id,
        b.shift,
        b.status,
        b.lag_status,
        CASE
            WHEN b.status = 'Up'::text AND b.lag_status = 'Down'::text THEN 'start'::text
            WHEN b.status = 'Down'::text AND b.lag_status = 'Up'::text THEN 'stop'::text
            WHEN b.date_time::time without time zone = '00:00:00'::time without time zone THEN 'day_change'::text
            WHEN b.date_time::time without time zone = ANY (ARRAY['08:00:00'::time without time zone, '20:00:00'::time without time zone]) THEN 'shift_change'::text
            ELSE NULL::text
        END AS flag
    FROM 
        b
), d AS (
    SELECT 
        c.date_time,
        c.iot_id,
        c.shift,
        CASE
            WHEN c.flag = 'day_change'::text THEN lag(c.flag) OVER (PARTITION BY c.iot_id ORDER BY c.date_time)
            WHEN c.flag = 'shift_change'::text THEN lag(c.flag) OVER (PARTITION BY c.iot_id ORDER BY c.date_time)
            ELSE c.flag
        END AS flag
    FROM 
        c
    WHERE 
        c.flag IS NOT NULL
), e AS (
    SELECT 
        d.date_time,
        d.iot_id,
        d.shift,
        d.flag,
        lag(d.flag) OVER (PARTITION BY d.iot_id ORDER BY d.date_time) AS lag_flag
    FROM 
        d
), f AS (
    SELECT 
        e.date_time,
        e.iot_id,
        e.shift,
        CASE
            WHEN e.flag = 'start'::text AND e.lag_flag = 'stop'::text THEN 'Up'::text
            WHEN e.flag = 'start'::text AND e.lag_flag = 'start'::text THEN 'Up'::text
            WHEN e.flag = 'stop'::text AND e.lag_flag = 'start' THEN 'Down'::text
            WHEN e.flag = 'stop'::text AND e.lag_flag = 'stop'::text THEN 'Down'::text
            ELSE NULL::text
        END AS status
    FROM 
        e
), final_raw AS (
    SELECT 
        f.date_time AS start_time,
        lead(f.date_time) OVER (PARTITION BY f.iot_id ORDER BY f.date_time) AS end_time,
        f.iot_id,
        f.shift,
        CASE
            WHEN f.status = 'Down'::text AND EXTRACT(epoch FROM lead(f.date_time) OVER (PARTITION BY f.iot_id ORDER BY f.date_time) - f.date_time) >= 60::numeric THEN 'Down'::text
            WHEN f.status = 'Down'::text AND EXTRACT(epoch FROM lead(f.date_time) OVER (PARTITION BY f.iot_id ORDER BY f.date_time) - f.date_time) < 60::numeric THEN lag(f.status) OVER (PARTITION BY f.iot_id ORDER BY f.date_time)
            ELSE f.status
        END AS status,
        EXTRACT(epoch FROM lead(f.date_time) OVER (PARTITION BY f.iot_id ORDER BY f.date_time) - f.date_time) / 3600::numeric AS duration_hrs,
        EXTRACT(epoch FROM lead(f.date_time) OVER (PARTITION BY f.iot_id ORDER BY f.date_time) - f.date_time) * 1000::numeric AS duration_ms
    FROM 
        f
    ORDER BY 
        f.date_time
), final_summary AS (
    SELECT 
        date(start_time) AS day,
        shift,
        status,
        SUM(duration_hrs) AS total_duration_hrs
    FROM 
        final_raw
    WHERE 
        iot_id = 'MX02202335'
    GROUP BY 
        day, shift, status
)
SELECT 
    day, 
    shift, 
    status, 
    total_duration_hrs
FROM 
    final_summary;
