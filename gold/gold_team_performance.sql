CREATE OR REFRESH MATERIALIZED VIEW f1.gold_sch.gold_team_performance AS
select 
    d.team_name,
    array_join(collect_set(d.full_name), ', ') AS driver_names,
    COUNT(DISTINCT CASE
            WHEN p.position = 1 THEN p.session_key
        END) AS total_wins, 
        count(distinct CASE WHEN p.position <= 3 THEN p.session_key END) as podium_finishes,
    extract(year from p.date) as year
    from f1.silver_sch.silver_drivers d
    join f1.silver_sch.silver_position p
    on d.session_key = p.session_key
    and d.driver_number = p.driver_number
    group by d.team_name, year
    order by year desc, total_wins desc;