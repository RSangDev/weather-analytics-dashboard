"""
Airflow DAG for weather data pipeline automation.
Runs daily to fetch, process, and store weather data.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import yaml
import pandas as pd
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from data.fetcher import WeatherDataFetcher  # noqa 
from processing.analyzer import WeatherDataProcessor  # noqa


def load_config():
    """Load configuration."""
    config_path = Path(__file__).parent.parent / "config" / "config.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def fetch_weather_data(**context):
    """Task to fetch weather data."""
    config = load_config()
    fetcher = WeatherDataFetcher(config)

    raw_data = fetcher.fetch_all_cities(
        config["cities"], config["processing"]["forecast_days"]
    )

    # Push to XCom for next task
    context["task_instance"].xcom_push(key="raw_data", value=raw_data)

    return f"Fetched data for {len(raw_data)} cities"


def process_weather_data(**context):
    """Task to process weather data."""
    config = load_config()
    processor = WeatherDataProcessor(config)

    # Pull from XCom
    raw_data = context["task_instance"].xcom_pull(task_ids="fetch_data", key="raw_data")

    # Process data
    df = processor.raw_to_dataframe(raw_data)
    df = processor.calculate_moving_averages(df)
    df = processor.detect_anomalies(df)

    # Generate alerts
    alerts = processor.generate_alerts(df)

    # Push processed data
    context["task_instance"].xcom_push(key="processed_data", value=df.to_dict())
    context["task_instance"].xcom_push(key="alerts", value=alerts)

    return f"Processed {len(df)} records, generated {len(alerts)} alerts"


def store_results(**context):
    """Task to store results."""
    # Pull from XCom
    processed_data = context["task_instance"].xcom_pull(
        task_ids="process_data", key="processed_data"
    )
    alerts = context["task_instance"].xcom_pull(task_ids="process_data", key="alerts")

    # Convert back to DataFrame
    df = pd.DataFrame(processed_data)

    # Store to CSV (in production, use database)
    output_dir = Path(__file__).parent.parent / "data"
    output_dir.mkdir(exist_ok=True)

    timestamp = context["execution_date"].strftime("%Y%m%d_%H%M%S")
    df.to_csv(output_dir / f"weather_data_{timestamp}.csv", index=False)

    # Store alerts
    if alerts:
        alerts_df = pd.DataFrame(alerts)
        alerts_df.to_csv(output_dir / f"alerts_{timestamp}.csv", index=False)

    return f"Stored {len(df)} records and {len(alerts)} alerts"


def send_alert_notifications(**context):
    """Task to send notifications for critical alerts."""
    alerts = context["task_instance"].xcom_pull(task_ids="process_data", key="alerts")

    critical_alerts = [
        alert for alert in alerts if alert["type"] in ["high_temperature", "high_wind"]
    ]

    if critical_alerts:
        # In production, send email/SMS notifications
        print(f"CRITICAL: {len(critical_alerts)} critical alerts detected!")
        for alert in critical_alerts[:5]:  # Show first 5
            print(f"  - {alert['message']}")

    return f"Processed {len(critical_alerts)} critical alerts"


# Default arguments
default_args = {
    "owner": "weather-analytics",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    "weather_analytics_pipeline",
    default_args=default_args,
    description="Automated weather data collection and analysis pipeline",
    schedule_interval="0 */6 * * *",  # Run every 6 hours
    start_date=days_ago(1),
    catchup=False,
    tags=["weather", "analytics", "monitoring"],
)

# Define tasks
fetch_task = PythonOperator(
    task_id="fetch_data",
    python_callable=fetch_weather_data,
    dag=dag,
)

process_task = PythonOperator(
    task_id="process_data",
    python_callable=process_weather_data,
    dag=dag,
)

store_task = PythonOperator(
    task_id="store_results",
    python_callable=store_results,
    dag=dag,
)

notify_task = PythonOperator(
    task_id="send_notifications",
    python_callable=send_alert_notifications,
    dag=dag,
)

# Set task dependencies
fetch_task >> process_task >> [store_task, notify_task]
