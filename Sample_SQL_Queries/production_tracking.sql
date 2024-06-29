WITH sample_master_mapping_temp AS (
    SELECT 
        sample_master_mapping.machine_name::text AS machine_code,
        sample_master_mapping.device_uid::text AS device_uid,
        round(sample_master_mapping.size_range1::numeric, 2) AS size_range1,
        round(sample_master_mapping.size_range2::numeric, 2) AS size_range2,
        sample_master_mapping.product_code::text AS product_code,
        sample_master_mapping.speed::numeric AS speed
    FROM 
        sample_master_mapping
),
sample_act_prod AS (
    SELECT 
        sample_act_prod.date::date AS date,
        sample_act_prod.date_time,
        sample_act_prod.supervisor_name,
        CASE
            WHEN sample_act_prod.machine_code = '10H-3'::text THEN 'MX02202335'::text
            WHEN sample_act_prod.machine_code = '6H'::text THEN 'MX02202330'::text
            ELSE NULL::text
        END AS device_uid,
        CASE
            WHEN sample_act_prod.shift = 'A'::text THEN 'shift_a'::text
            WHEN sample_act_prod.shift = 'B'::text THEN 'shift_b'::text
            ELSE NULL::text
        END AS shift,
        sample_act_prod.product_code::text AS product_code,
        round(sample_act_prod.wire_size::numeric, 2) AS wire_size,
        sample_act_prod.act_prod::numeric AS act_prod,
        CASE
            WHEN sample_act_prod.rejected_prod = ''::text THEN 0::numeric
            ELSE sample_act_prod.rejected_prod::numeric
        END AS rejected_prod
    FROM 
        public.sample_act_prod
),
main_prod AS (
    SELECT 
        ap.date,
        ap.date_time,
        ap.supervisor_name,
        mm.machine_code,
        ap.device_uid,
        ap.shift,
        ap.product_code,
        ap.wire_size,
        mm.speed,
        ap.act_prod,
        ap.rejected_prod
    FROM 
        sample_act_prod ap
    LEFT JOIN 
        sample_master_mapping_temp mm 
    ON 
        ap.wire_size >= mm.size_range1 
        AND ap.wire_size <= mm.size_range2 
        AND ap.product_code = mm.product_code 
        AND ap.device_uid = mm.device_uid
),
machine_oee_temp AS (
    SELECT 
        dl.start_time::date AS date,
        dl.shift,
        CASE
            WHEN dl.shift = 'shift_a'::text THEN 'shift_a'::text
            WHEN dl.shift = 'shift_b1'::text THEN 'shift_b'::text
            WHEN dl.shift = 'shift_b2'::text THEN 'shift_b'::text
            ELSE NULL::text
        END AS shift_new,
        dl.device_uid,
        sum(dl.duration_hrs) AS duration_hrs,
        CASE
            WHEN dl.status = 'Up'::text THEN dl.duration_hrs
            ELSE 0::numeric
        END AS runtime,
        CASE
            WHEN dl.status = 'Down'::text THEN dl.duration_hrs
            ELSE 0::numeric
        END AS downtime,
        CASE
            WHEN dl.shift = 'shift_a'::text THEN 12::numeric
            WHEN dl.shift = 'shift_b1'::text THEN 4::numeric
            WHEN dl.shift = 'shift_b2'::text THEN 8::numeric
            ELSE NULL::numeric
        END AS planned_time
    FROM 
        sample_down_log dl
    GROUP BY 
        dl.start_time::date, 
        dl.shift, 
        dl.device_uid
),
machine_oee AS (
    SELECT 
        machine_oee_temp.date,
        machine_oee_temp.shift_new AS shift,
        machine_oee_temp.device_uid,
        sum(machine_oee_temp.runtime) AS runtime,
        sum(machine_oee_temp.downtime) AS downtime,
        sum(machine_oee_temp.planned_time) OVER (PARTITION BY machine_oee_temp.date, machine_oee_temp.shift_new, machine_oee_temp.device_uid) AS planned_time
    FROM 
        machine_oee_temp
    GROUP BY 
        machine_oee_temp.date, 
        machine_oee_temp.shift_new, 
        machine_oee_temp.device_uid, 
        machine_oee_temp.planned_time
),
machine_oee_final AS (
    SELECT 
        machine_oee.date,
        machine_oee.shift,
        machine_oee.device_uid,
        sum(machine_oee.runtime) AS runtime,
        sum(machine_oee.downtime) AS downtime,
        max(machine_oee.planned_time) AS planned_time
    FROM 
        machine_oee
    GROUP BY 
        machine_oee.date, 
        machine_oee.shift, 
        machine_oee.device_uid
)
SELECT 
    mo.date,
    mo.shift,
    mo.device_uid,
    mp.product_code,
    mp.wire_size,
    mo.runtime,
    mo.downtime,
    mo.planned_time,
    mp.speed,
    round(mp.wire_size * mp.wire_size * mo.planned_time * mp.speed * 0.3738, 0) AS std_prod,
    mp.act_prod,
    mp.rejected_prod
FROM 
    machine_oee_final mo
JOIN 
    main_prod mp 
ON 
    mo.device_uid = mp.device_uid 
    AND mo.shift = mp.shift 
    AND mo.date = mp.date
WHERE 
    mo.device_uid = 'MX02202335';
