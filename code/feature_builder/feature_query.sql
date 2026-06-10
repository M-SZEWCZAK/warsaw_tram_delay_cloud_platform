-- feature_builder/feature_query.sql
-- BigQuery Scheduled Query — runs daily at 02:00 (before Timetable Loader fires).
-- Materialises a 30-day rolling feature table used by the Training Job.
--
-- Destination table: <project>.trams_warsaw.features
-- Write disposition: WRITE_TRUNCATE (full refresh each day)
--
-- Clustering: line, hour_of_day  (matches Prediction API query pattern)

DECLARE window_start DATE DEFAULT DATE_SUB(CURRENT_DATE('Europe/Warsaw'), INTERVAL 30 DAY);
DECLARE window_end   DATE DEFAULT DATE_SUB(CURRENT_DATE('Europe/Warsaw'), INTERVAL 1  DAY);

CREATE OR REPLACE TABLE `@GCP_PROJECT_ID.trams_warsaw.features`
CLUSTER BY line, hour_of_day
AS

WITH base AS (
  SELECT
    line,
    brigade,
    matched_stop_id                               AS stop_id,
    EXTRACT(HOUR FROM gps_time AT TIME ZONE 'Europe/Warsaw') AS hour_of_day,
    delay_s,
    precip_mm,
    DATE(gps_time AT TIME ZONE 'Europe/Warsaw')   AS obs_date
  FROM `@GCP_PROJECT_ID.trams_warsaw.positions_enriched`
  WHERE
    DATE(gps_time AT TIME ZONE 'Europe/Warsaw') BETWEEN window_start AND window_end
    AND matched_stop_id IS NOT NULL
    AND delay_s IS NOT NULL
),

aggregated AS (
  SELECT
    line,
    brigade,
    stop_id,
    hour_of_day,
    AVG(delay_s)                                                          AS mean_delay_s,
    STDDEV_SAMP(delay_s)                                                  AS stddev_delay_s,
    -- Rain–delay Pearson correlation (requires > 1 sample)
    CORR(COALESCE(precip_mm, 0), CAST(delay_s AS FLOAT64))               AS rain_delay_corr,
    -- Peak hour: 7–9 and 16–18 Warsaw time
    LOGICAL_OR(hour_of_day BETWEEN 7 AND 9 OR hour_of_day BETWEEN 16 AND 18) AS peak_hour_flag,
    COUNT(*)                                                              AS sample_count,
    window_start                                                          AS window_start,
    window_end                                                            AS window_end,
    CURRENT_TIMESTAMP()                                                   AS computed_at
  FROM base
  GROUP BY line, brigade, stop_id, hour_of_day
)

SELECT * FROM aggregated
WHERE sample_count >= 5   -- discard cells with too few observations
;
