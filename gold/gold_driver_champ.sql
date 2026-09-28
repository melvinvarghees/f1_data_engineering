 -- Gold layer: Driver championship standings with driver and team details
-- Source: silver_championship_drivers joined with silver_drivers

CREATE OR REFRESH MATERIALIZED VIEW f1.gold_sch.gold_driver_championship AS
SELECT
    
    c.session_key,
    c.position_current AS position,
    c.driver_number,
    d.full_name AS driver_name,
    d.team_name,
    c.points_current AS points,
    s.year AS championship_year
FROM f1.silver_sch.silver_championship_drivers c
LEFT JOIN f1.silver_sch.silver_drivers d
    ON c.driver_number = d.driver_number
   AND c.session_key = d.session_key
LEFT JOIN f1.silver_sch.silver_sessions s
    ON c.session_key = s.session_key