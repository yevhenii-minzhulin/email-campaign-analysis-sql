-- Calculate engagement rates
SELECT
    country,
    COUNT(DISTINCT sent_id) AS sent,
    COUNT(DISTINCT open_id) AS opened,
    SAFE_DIVIDE(COUNT(DISTINCT open_id), COUNT(DISTINCT sent_id)) AS open_rate
FROM ...
GROUP BY country;
