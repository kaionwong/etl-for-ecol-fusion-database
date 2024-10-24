WITH CollisionCutoffDates AS (
    -- CTE that defines cutoff dates for each created year. 
    -- Each record specifies a year and the corresponding maximum date until which collisions are considered valid.
    SELECT 2024 AS created_year, TO_DATE('2026-06-30', 'YYYY-MM-DD') AS cutoff_end_date UNION ALL
    SELECT 2023 AS created_year, TO_DATE('2025-06-30', 'YYYY-MM-DD') AS cutoff_end_date UNION ALL
    SELECT 2022 AS created_year, TO_DATE('2024-06-30', 'YYYY-MM-DD') AS cutoff_end_date UNION ALL
    SELECT 2021 AS created_year, TO_DATE('2023-02-06', 'YYYY-MM-DD') AS cutoff_end_date UNION ALL
    SELECT 2020 AS created_year, TO_DATE('2022-06-15', 'YYYY-MM-DD') AS cutoff_end_date UNION ALL
    SELECT 2019 AS created_year, TO_DATE('2021-10-23', 'YYYY-MM-DD') AS cutoff_end_date UNION ALL
    SELECT 2018 AS created_year, TO_DATE('2020-01-23', 'YYYY-MM-DD') AS cutoff_end_date UNION ALL
    SELECT 2017 AS created_year, TO_DATE('2019-02-11', 'YYYY-MM-DD') AS cutoff_end_date UNION ALL
    SELECT 2016 AS created_year, TO_DATE('2018-01-26', 'YYYY-MM-DD') AS cutoff_end_date UNION ALL
    SELECT 2015 AS created_year, TO_DATE('2016-01-02', 'YYYY-MM-DD') AS cutoff_end_date UNION ALL
    SELECT 2014 AS created_year, TO_DATE('2015-01-02', 'YYYY-MM-DD') AS cutoff_end_date UNION ALL
    SELECT 2013 AS created_year, TO_DATE('2014-01-02', 'YYYY-MM-DD') AS cutoff_end_date UNION ALL
    SELECT 2012 AS created_year, TO_DATE('2013-01-02', 'YYYY-MM-DD') AS cutoff_end_date UNION ALL
    SELECT 2011 AS created_year, TO_DATE('2012-01-02', 'YYYY-MM-DD') AS cutoff_end_date UNION ALL
    SELECT 2010 AS created_year, TO_DATE('2011-01-02', 'YYYY-MM-DD') AS cutoff_end_date UNION ALL
    SELECT 2009 AS created_year, TO_DATE('2010-01-02', 'YYYY-MM-DD') AS cutoff_end_date UNION ALL
    SELECT 2008 AS created_year, TO_DATE('2009-01-02', 'YYYY-MM-DD') AS cutoff_end_date UNION ALL
    SELECT 2007 AS created_year, TO_DATE('2008-01-02', 'YYYY-MM-DD') AS cutoff_end_date UNION ALL
    SELECT 2006 AS created_year, TO_DATE('2007-01-02', 'YYYY-MM-DD') AS cutoff_end_date UNION ALL
    SELECT 2005 AS created_year, TO_DATE('2006-01-02', 'YYYY-MM-DD') AS cutoff_end_date UNION ALL
    SELECT 2004 AS created_year, TO_DATE('2005-01-02', 'YYYY-MM-DD') AS cutoff_end_date
),
CollisionEarliestDate AS (
    -- CTE that retrieves the earliest creation date for each collision.
    -- It groups the records by collision_id and calculates the minimum created_timestamp for each collision.
    SELECT
        collision_id,
        TO_CHAR(MIN(created_timestamp), 'YYYY-MM-DD') AS earliest_created_date
    FROM
        public.oracle_cl_status_history
    GROUP BY
        collision_id
),
CollisionCaseYear AS (
    -- CTE that extracts the year from the earliest created date for each collision.
    -- It uses the results from CollisionEarliestDate to assign a created_year to each collision_id.
    SELECT
        ced.collision_id,
        EXTRACT(YEAR FROM TO_DATE(ced.earliest_created_date, 'YYYY-MM-DD')) AS created_year,
        ced.earliest_created_date
    FROM
        CollisionEarliestDate ced
),
CollisionWithCutoff AS (
    -- CTE that joins the collision case years with the cutoff dates.
    -- This provides each collision with the corresponding cutoff end date based on its created year.
    SELECT
        ccy.collision_id,
        ccy.created_year,
        ccd.cutoff_end_date
    FROM
        CollisionCaseYear ccy
    JOIN CollisionCutoffDates ccd ON ccy.created_year = ccd.created_year
),
CollisionStatusOnCutoff AS (
    -- CTE that retrieves the status of collisions at or before their respective cutoff dates.
    -- It partitions the results by collision_id and orders them by effective_date and coll_status_type_id.
    SELECT
        cwc.collision_id,
        cwc.created_year,
        cwc.cutoff_end_date,
        csh.coll_status_type_id,
        csh.effective_date,
        ROW_NUMBER() OVER (
            PARTITION BY cwc.collision_id
            ORDER BY csh.effective_date DESC, csh.coll_status_type_id DESC
        ) AS rn
    FROM
        CollisionWithCutoff cwc
    JOIN public.oracle_cl_status_history csh ON cwc.collision_id = csh.collision_id
    AND csh.effective_date::DATE <= cwc.cutoff_end_date
    WHERE csh.effective_date::DATE <= cwc.cutoff_end_date
),
CollisionStatusOnCutoffFiltered AS (
    -- CTE that filters the statuses obtained in the previous CTE to ensure effective dates are valid.
    -- This prepares the data for further filtering and ranking.
    SELECT *
    FROM CollisionStatusOnCutoff
    WHERE effective_date <= cutoff_end_date
),
CollisionStatusOnCutoffFilteredTwice AS (
    -- CTE that assigns a second row number (rn2) for further filtering.
    -- It reorders the results while still keeping the first row per collision for easy access.
    SELECT
        collision_id,
        created_year,
        cutoff_end_date,
        coll_status_type_id,
        effective_date,
        rn,
        ROW_NUMBER() OVER (
            PARTITION BY collision_id
            ORDER BY rn ASC
        ) AS rn2
    FROM CollisionStatusOnCutoffFiltered
),
CollisionStatusOnCutoffFilteredThrice AS (
    -- CTE that filters the results to keep only the most relevant status for each collision.
    -- It ensures that only the top-ranked status (rn2 = 1) is included in the final results.
    SELECT *
    FROM CollisionStatusOnCutoffFilteredTwice
    WHERE rn2 = 1
)

----------------
--- Option 1 ---

-- SELECT
--     csoc.collision_id,
--     csoc.created_year,
--     EXTRACT(YEAR FROM c.occurence_timestamp) AS case_year,
--     csoc.cutoff_end_date,
--     csoc.coll_status_type_id,
--     csoc.effective_date,
--     c.case_nbr,
--     c.pfn_file_nbr,
--     c.occurence_timestamp,
--     c.reported_timestamp,
--     CASE
--         WHEN csoc.coll_status_type_id = 220 THEN 1 -- 220 as upload pending
--         WHEN csoc.coll_status_type_id = 221 THEN 1 -- 221 as uploaded
--         ELSE 0
--     END AS valid_at_cutoff_flag
-- FROM
--     CollisionStatusOnCutoffFilteredThrice csoc
--     LEFT JOIN public.oracle_collisions c ON csoc.collision_id = c.id
-- ORDER BY
--     csoc.collision_id;

--- Option 1 ends ---
---------------------

----------------
--- Option 2 ---

SELECT
    c.case_nbr
    -- ,csoc.collision_id
    -- ,csoc.created_year
    -- ,EXTRACT(YEAR FROM c.occurence_timestamp) AS case_year
    -- ,csoc.cutoff_end_date
    -- ,csoc.coll_status_type_id
    -- ,csoc.effective_date
    -- ,c.pfn_file_nbr
    -- ,c.occurence_timestamp
    -- ,c.reported_timestamp
FROM
    CollisionStatusOnCutoffFilteredThrice csoc
    LEFT JOIN public.oracle_collisions c ON csoc.collision_id = c.id
WHERE 1=1
    AND (csoc.coll_status_type_id = 220 OR csoc.coll_status_type_id = 221)  -- Check for valid status
    --AND EXTRACT(YEAR FROM c.occurence_timestamp) = 2020  -- Filter by case year
	AND c.case_nbr IS NOT NULL
ORDER BY
    c.case_nbr DESC;

--- Option 2 ends ---
---------------------
