-- обрахунок основних метрик емейлів
WITH email_metrics AS (
    SELECT
        DATE_ADD(s.date, INTERVAL es.sent_date DAY) AS sent_date,
        sp.country AS country,
        acc.send_interval AS send_interval,
        acc.is_verified AS is_verified,
        acc.is_unsubscribed AS is_unsubscribed,
        COUNT(DISTINCT es.id_message) AS sent_msg,
        COUNT(DISTINCT eo.id_message) AS open_msg,
        COUNT(DISTINCT ev.id_message) AS visit_msg
    FROM `DA.email_sent` es
    JOIN `DA.account_session` acs
        ON es.id_account = acs.account_id
    JOIN `DA.session` s
        ON s.ga_session_id = acs.ga_session_id
    JOIN `DA.session_params` sp
        ON sp.ga_session_id = s.ga_session_id
    JOIN `DA.account` acc
        ON acc.id = acs.account_id
    LEFT JOIN `DA.email_open` eo
        ON es.id_message = eo.id_message
    LEFT JOIN `DA.email_visit` ev
        ON es.id_message = ev.id_message
    GROUP BY
        DATE_ADD(s.date, INTERVAL es.sent_date DAY),
        sp.country,
        acc.send_interval,
        acc.is_verified,
        acc.is_unsubscribed
),


-- обрахунок основних метрик акаунтів
account_metrics AS (
    SELECT
        DATE_ADD(s.date, INTERVAL acc.send_interval DAY) AS sent_date,
        sp.country AS country,
        acc.send_interval AS send_interval,
        acc.is_verified AS is_verified,
        acc.is_unsubscribed AS is_unsubscribed,
        COUNT(DISTINCT acc.id) AS account_cnt
    FROM `DA.account` acc
    JOIN `DA.account_session` acs
        ON acc.id = acs.account_id
    JOIN `DA.session` s
        ON acs.ga_session_id = s.ga_session_id
    JOIN `DA.session_params` sp
        ON sp.ga_session_id = acs.ga_session_id
    GROUP BY
        DATE_ADD(s.date, INTERVAL acc.send_interval DAY),
        sp.country,
        acc.send_interval,
        acc.is_verified,
        acc.is_unsubscribed
),


-- об'єднання даних з обох CTE
union_data AS (
    SELECT sent_date,
           country,
           send_interval,
           is_verified,
           is_unsubscribed,
           sent_msg,
           open_msg,
           visit_msg,
           0 AS account_cnt
    FROM email_metrics
    UNION ALL
    SELECT sent_date,
           country,
           send_interval,
           is_verified,
           is_unsubscribed,
           0 AS sent_msg,
           0 AS open_msg,
           0 AS visit_msg,
           account_cnt
    FROM account_metrics
),


-- повторна агрегація після об'єднання
aggregated_union AS (
    SELECT
        sent_date,
        country,
        send_interval,
        is_verified,
        is_unsubscribed,
        SUM(account_cnt) AS account_cnt,
        SUM(sent_msg) AS sent_msg,
        SUM(open_msg) AS open_msg,
        SUM(visit_msg) AS visit_msg
    FROM union_data
    GROUP BY
        sent_date,
        country,
        send_interval,
        is_verified,
        is_unsubscribed
),


-- обрахунок загальної кількості акаунтів та відправлених емейлів у розрізі країн
totals AS (
    SELECT *,
           SUM(account_cnt) OVER (PARTITION BY country) AS total_country_account_cnt,
           SUM(sent_msg) OVER (PARTITION BY country) AS total_country_sent_cnt
    FROM aggregated_union
),


-- рейтинг загальної кількості акаунтів та відправлених емейлів у розрізі країн
ranks AS (
    SELECT *,
           DENSE_RANK() OVER (ORDER BY total_country_account_cnt DESC) AS rank_total_country_account_cnt,
           DENSE_RANK() OVER (ORDER BY total_country_sent_cnt DESC) AS rank_total_country_sent_cnt
    FROM totals
)


-- підсумкове виведення даних
SELECT
    sent_date AS date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    account_cnt,
    sent_msg,
    open_msg,
    visit_msg,
    total_country_account_cnt,
    total_country_sent_cnt,
    rank_total_country_account_cnt,
    rank_total_country_sent_cnt
FROM ranks
WHERE rank_total_country_account_cnt <= 10 OR rank_total_country_sent_cnt <= 10;

