-- Preferred unit names used throughout the repository layer.
CREATE OR REPLACE VIEW hgis.vob_preferred_unit_name AS
SELECT
    ranked.g_unit,
    ranked.g_name
FROM (
    SELECT
        n.g_unit,
        n.g_name,
        ROW_NUMBER() OVER (
            PARTITION BY n.g_unit
            ORDER BY
                CASE
                    WHEN n.g_language IS NOT NULL AND n.g_language = 'eng' THEN 0
                    ELSE 1
                END,
                n.g_name
        ) AS rn
    FROM hgis.g_name n
    WHERE n.g_name_status = 'P'
) ranked
WHERE ranked.rn = 1;


-- Theme/cube lookup shape reused by theme and series repositories.
CREATE OR REPLACE VIEW hgis.vob_theme_cube_summary AS
SELECT
    ncube.theme_id,
    ncube.ent_id AS cube_id,
    ncube.labl AS cube_label,
    ncube.text AS cube_text
FROM hgis.g_data_ent ncube
WHERE ncube.ent_type = 'N';


-- Stat-map feature base view that keeps geometry and preferred names together.
CREATE OR REPLACE VIEW hgis.vob_stat_map_feature AS
SELECT
    foot.g_unit,
    foot.g_unit_type,
    preferred.g_name AS unit_name,
    foot.g_foot_ertslcc,
    util.get_start_year(foot.g_duration) AS start_year,
    util.get_end_year(foot.g_duration) AS end_year
FROM hgis.g_foot foot
JOIN hgis.vob_preferred_unit_name preferred
    ON preferred.g_unit = foot.g_unit
WHERE foot.use_for_stat_map = 'Y';
