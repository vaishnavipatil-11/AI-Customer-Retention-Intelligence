SELECT COUNT(*) FROM saas_retention.accounts;
SELECT COUNT(*) FROM saas_retention.subscriptions;
SELECT COUNT(*) FROM saas_retention.feature_usage;
SELECT COUNT(*) FROM saas_retention.support_tickets;
SELECT COUNT(*) FROM saas_retention.churn_events;

SELECT account_id, COUNT(*)
FROM saas_retention.accounts
GROUP BY account_id
HAVING COUNT(*) > 1;

SELECT subscription_id, COUNT(*)
FROM saas_retention.subscriptions
GROUP BY subscription_id
HAVING COUNT(*) > 1;

SELECT usage_id, COUNT(*)
FROM saas_retention.feature_usage
GROUP BY usage_id
HAVING COUNT(*) > 1;

SELECT churn_event_id, COUNT(*) 
FROM saas_retention.churn_events
GROUP BY churn_event_id
HAVING COUNT(*) > 1;

SELECT s.subscription_id
FROM saas_retention.subscriptions s
LEFT JOIN saas_retention.accounts a
ON s.account_id = a.account_id
WHERE a.account_id IS NULL;

SELECT *
FROM saas_retention.feature_usage
WHERE usage_id = 'U-0c9318';

CREATE OR REPLACE VIEW
saas_retention.v_customer_health_metrics AS

WITH account_base AS (
  SELECT
    account_id,
    signup_date,
    DATE_DIFF(CURRENT_DATE(), signup_date, DAY) AS tenure_days
  FROM saas_retention.accounts
),

base_activity AS (
  SELECT
    a.account_id,
    fu.feature_name,
    fu.usage_duration_secs,
    fu.error_count
  FROM account_base a
  LEFT JOIN saas_retention.subscriptions s
    ON a.account_id = s.account_id
  LEFT JOIN saas_retention.feature_usage fu
    ON s.subscription_id = fu.subscription_id
),

account_usage_metrics AS (
  SELECT
    account_id,
    COALESCE(SUM(usage_duration_secs),0) AS activity_duration,
    COUNT(DISTINCT feature_name) AS tot_features_used,
    COALESCE(SUM(error_count),0) AS total_errors,
    COUNT(feature_name) AS total_usage_events
  FROM base_activity
  GROUP BY account_id
),

support_activity AS (
  SELECT
    a.account_id,
    COALESCE(AVG(b.satisfaction_score),0) AS avg_satisfaction_score,
    COUNT(b.ticket_id) AS total_tickets,
    SUM(CASE WHEN b.escalation_flag THEN 1 ELSE 0 END) AS esc_freq
  FROM saas_retention.accounts a
  LEFT JOIN saas_retention.support_tickets b
    ON a.account_id = b.account_id
  GROUP BY a.account_id
),

user_metrics AS (
  SELECT
    ab.account_id,
    ab.tenure_days,
    u.activity_duration,
    u.tot_features_used,
    u.total_errors,
    u.total_usage_events,
    COALESCE(SAFE_DIVIDE(u.total_errors, u.total_usage_events),0) AS error_rate,
    s.avg_satisfaction_score,
    s.total_tickets,
    s.esc_freq
FROM account_base ab
LEFT JOIN account_usage_metrics u
  ON ab.account_id = u.account_id
LEFT JOIN support_activity s
  ON ab.account_id = s.account_id
),

normalized_values AS (
  SELECT
    MIN(tenure_days) AS min_tenure,
    MAX(tenure_days) AS max_tenure,

    MIN(activity_duration) AS min_activity,
    MAX(activity_duration) AS max_activity,

    MIN(tot_features_used) AS min_features,
    MAX(tot_features_used) AS max_features,

    MIN(error_rate) AS min_error_rate,
    MAX(error_rate) AS max_error_rate,

    MIN(avg_satisfaction_score) AS min_satisfaction,
    MAX(avg_satisfaction_score) AS max_satisfaction,

    MIN(esc_freq) AS min_escalations,
    MAX(esc_freq) AS max_escalations

  FROM user_metrics
),

normalized_metrics AS (
  SELECT
    u.account_id,

    SAFE_DIVIDE(u.tenure_days - n.min_tenure,
                n.max_tenure - n.min_tenure) AS tenure_score,

    SAFE_DIVIDE(u.activity_duration - n.min_activity,
                n.max_activity - n.min_activity) AS activity_score,

    SAFE_DIVIDE(u.tot_features_used - n.min_features,
                n.max_features - n.min_features) AS feature_score,

    1 - SAFE_DIVIDE(u.error_rate - n.min_error_rate,
                    n.max_error_rate - n.min_error_rate) AS error_quality_score,

    SAFE_DIVIDE(u.avg_satisfaction_score - n.min_satisfaction,
                n.max_satisfaction - n.min_satisfaction) AS satisfaction_score,

    1 - SAFE_DIVIDE(u.esc_freq - n.min_escalations,
                    n.max_escalations - n.min_escalations) AS escalation_score

  FROM user_metrics u
  CROSS JOIN normalized_values n
),

scores AS (
  SELECT 
    account_id,
    ROUND((activity_score + feature_score + error_quality_score)/3, 4) AS usage_score,
    ROUND((satisfaction_score + escalation_score)/2, 4) AS sentiment_score,
    ROUND(tenure_score, 4) AS tenure_score
  FROM normalized_metrics
),

health_scores AS (
  SELECT
    account_id,
    usage_score,
    sentiment_score,
    tenure_score,
    ROUND((usage_score * 0.4 +
           sentiment_score * 0.4 +
           tenure_score * 0.2), 3) AS health_score
  FROM scores
)

SELECT
  account_id,
  usage_score,
  sentiment_score,
  tenure_score,
  health_score,
  CASE
    WHEN health_score >= 0.7 THEN 'Healthy'
    WHEN health_score >= 0.4 THEN 'Warning'
    ELSE 'Critical'
  END AS risk_segment
FROM health_scores
ORDER BY health_score ASC;

select *
from  saas_retention.v_customer_health_metrics;

SELECT risk_segment, COUNT(*)
FROM saas_retention.v_customer_health_metrics
GROUP BY risk_segment;

SELECT *
FROM saas_retention.v_customer_health_metrics
WHERE risk_segment = 'Critical'
ORDER BY health_score
LIMIT 10;

CREATE OR REPLACE TABLE saas_retention.retention_actions_log (
  action_id STRING,
  account_id STRING,
  health_score FLOAT64,
  risk_segment STRING,
  recovery_strategy STRING,
  email_draft STRING,
  generated_at TIMESTAMP,
  action_status STRING
);









  










