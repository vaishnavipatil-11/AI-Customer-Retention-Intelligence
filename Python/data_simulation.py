# In[1]:


from google.cloud import bigquery
from google import genai
from datetime import datetime, timezone, date
import pandas as pd
import random
import uuid
import os



# In[2]:


client = bigquery.Client()
client_ai = genai.Client(GENAI_API_KEY = os.getenv("GENAI_API_KEY")) 


# In[3]:


def get_eligible_critical_accounts():

    query = """
        SELECT
          h.account_id,
          h.health_score,
          h.usage_score,
          h.sentiment_score,
          h.tenure_score
        FROM `saas_retention.v_customer_health_metrics` h
        LEFT JOIN `saas_retention.retention_actions_log` r
          ON h.account_id = r.account_id
          AND r.generated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
        WHERE h.risk_segment = 'Critical'
          AND r.account_id IS NULL
    """

    return client.query(query).to_dataframe()


# In[4]:


eligible_df = get_eligible_critical_accounts()


# In[7]:


def generate_email(row):

    prompt = f"""
You are a SaaS marketing specialist.

Account ID: {row['account_id']}
Health Score: {row['health_score']}
Usage Score: {row['usage_score']}
Sentiment Score: {row['sentiment_score']}
Tenure Score: {row['tenure_score']}

Write a concise professional retention campaign email.
No calls or meetings.
Email tone only.
"""

    response = client_ai.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt
    )

    return response.text


# In[11]:


def log_retention_action(account_id, health_score, email_text):

    data = [{
        "account_id": account_id,
        "health_score": health_score,
        "risk_segment": "Critical",
        "recovery_strategy": "email",
        "email_draft": email_text,
        "generated_at": datetime.now(timezone.utc)
    }]

    df = pd.DataFrame(data)

    table_id = "exalted-tape-489318-u8.saas_retention.retention_actions_log"

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"Logged retention action for {account_id}")


# In[12]:


if not eligible_df.empty:
    row = eligible_df.iloc[0]
    email = generate_email(row)
    log_retention_action(row["account_id"], row["health_score"], email)


# In[15]:


def snapshot_health_scores():

    query = """
        SELECT
            account_id,
            usage_score,
            sentiment_score,
            tenure_score,
            health_score,
            risk_segment
        FROM `saas_retention.v_customer_health_metrics`
    """

    df = client.query(query).to_dataframe()

    df["snapshot_date"] = date.today()

    df = df[
        [
            "snapshot_date",
            "account_id",
            "usage_score",
            "sentiment_score",
            "tenure_score",
            "health_score",
            "risk_segment"
        ]
    ]

    table_id = "exalted-tape-489318-u8.saas_retention.customer_health_history"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"Snapshot stored for {len(df)} accounts.")


# In[17]:


def simulate_daily_activity():

    # Pull active subscriptions
    query = """
        SELECT subscription_id
        FROM `saas_retention.subscriptions`
        WHERE end_date IS NULL
    """

    subs_df = client.query(query).to_dataframe()

    if subs_df.empty:
        print("No active subscriptions found.")
        return

    # Randomly select subset
    active_today = subs_df.sample(n=min(50, len(subs_df)))

    feature_pool = [
        "dashboard_view",
        "report_export",
        "data_upload",
        "analytics_insight",
        "settings_update",
        "api_access",
        "team_collaboration",
        "billing_page"
    ]

    rows = []

    for subscription_id in active_today["subscription_id"]:

        events_per_user = random.randint(1, 5)

        for _ in range(events_per_user):
            rows.append({
                "usage_id": str(uuid.uuid4()),
                "subscription_id": subscription_id,
                "usage_date": date.today(),
                "feature_name": random.choice(feature_pool),
                "usage_count": random.randint(1, 10),
                "usage_duration_secs": random.randint(30, 600),
                "error_count": random.choices([0, 1], weights=[0.85, 0.15])[0],
                "is_beta_feature": random.choices([True, False], weights=[0.1, 0.9])[0]
            })

    df = pd.DataFrame(rows)

    table_id = "exalted-tape-489318-u8.saas_retention.feature_usage"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"Inserted {len(df)} usage events.")


# In[13]:


def run_retention_trigger():

    eligible_df = get_eligible_critical_accounts()

    if eligible_df.empty:
        print("No eligible accounts today.")
        return

    print(f"{len(eligible_df)} accounts eligible for retention.")

    for _, row in eligible_df.iterrows():
        email = generate_email(row)

        log_retention_action(
            account_id=row["account_id"],
            health_score=row["health_score"],
            email_text=email
        )

    print("Retention trigger completed.")


# In[19]:


def run_daily_retention_pipeline():

    print("---- START ----")

    simulate_daily_activity()
    snapshot_health_scores()
    run_retention_trigger()

    print("---- COMPLETE ----")


# In[21]:


if __name__ == "__main__":
    run_daily_retention_pipeline()

