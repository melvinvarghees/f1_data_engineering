CREATE OR REFRESH MATERIALIZED VIEW f1.gold_sch.gold_driver_performance_summary AS

WITH driver_base AS (

    SELECT DISTINCT
        d.driver_number,
        d.full_name AS driver_name,
        d.team_name,
        s.year AS race_year
    FROM f1.silver_sch.silver_drivers d
    INNER JOIN f1.silver_sch.silver_sessions s
        ON d.session_key = s.session_key
),

race_results AS (

    SELECT
        p.driver_number,
        s.year AS race_year,

        COUNT(DISTINCT CASE
            WHEN p.position = 1 THEN p.session_key
        END) AS wins,

        COUNT(DISTINCT CASE
            WHEN p.position <= 3 THEN p.session_key
        END) AS podium_finishes,

        COUNT(DISTINCT p.session_key) AS total_races,

        ROUND(AVG(p.position), 2) AS avg_finish_position

    FROM f1.silver_sch.silver_position p
    INNER JOIN f1.silver_sch.silver_sessions s
        ON p.session_key = s.session_key

    WHERE s.session_name = 'Race'

    GROUP BY
        p.driver_number,
        s.year
),

fastest_laps AS (

    WITH ranked_laps AS (

        SELECT
            l.driver_number,
            s.year AS race_year,
            l.session_key,

            ROW_NUMBER() OVER (
                PARTITION BY l.session_key
                ORDER BY l.lap_duration
            ) AS rn

        FROM f1.silver_sch.silver_laps l
        INNER JOIN f1.silver_sch.silver_sessions s
            ON l.session_key = s.session_key

        WHERE s.session_name = 'Race'
          AND l.lap_duration IS NOT NULL
    )

    SELECT
        driver_number,
        race_year,
        COUNT(*) AS fastest_laps
    FROM ranked_laps
    WHERE rn = 1
    GROUP BY
        driver_number,
        race_year
),

best_sectors AS (

    SELECT
        l.driver_number,
        s.year AS race_year,

        MIN(l.duration_sector_1) AS best_sector_1_time,
        MIN(l.duration_sector_2) AS best_sector_2_time,
        MIN(l.duration_sector_3) AS best_sector_3_time,

        MIN(l.lap_duration) AS best_lap_time,

        COUNT(*) AS total_laps_completed

    FROM f1.silver_sch.silver_laps l
    INNER JOIN f1.silver_sch.silver_sessions s
        ON l.session_key = s.session_key

    WHERE l.duration_sector_1 IS NOT NULL
      AND l.duration_sector_2 IS NOT NULL
      AND l.duration_sector_3 IS NOT NULL
      AND l.lap_duration IS NOT NULL

    GROUP BY
        l.driver_number,
        s.year
)

SELECT

    d.driver_number,
    d.driver_name,
    d.team_name,
    d.race_year,

    COALESCE(rr.wins, 0) AS wins,

    COALESCE(rr.podium_finishes, 0) AS podium_finishes,

    COALESCE(fl.fastest_laps, 0) AS fastest_laps,

    COALESCE(rr.total_races, 0) AS total_races,

    COALESCE(rr.avg_finish_position, 0.0) AS avg_finish_position,

    COALESCE(bs.best_sector_1_time, 0.0) AS best_sector_1_time,

    COALESCE(bs.best_sector_2_time, 0.0) AS best_sector_2_time,

    COALESCE(bs.best_sector_3_time, 0.0) AS best_sector_3_time,

    COALESCE(bs.best_lap_time, 0.0) AS best_lap_time,

    COALESCE(bs.total_laps_completed, 0) AS total_laps_completed

FROM driver_base d

LEFT JOIN race_results rr
    ON d.driver_number = rr.driver_number
   AND d.race_year = rr.race_year

LEFT JOIN fastest_laps fl
    ON d.driver_number = fl.driver_number
   AND d.race_year = fl.race_year

LEFT JOIN best_sectors bs
    ON d.driver_number = bs.driver_number
   AND d.race_year = bs.race_year;