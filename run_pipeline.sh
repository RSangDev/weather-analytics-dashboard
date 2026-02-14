#!/bin/bash

# Weather Data Collection Cron Job
# Add to crontab with: 0 */6 * * * /path/to/run_pipeline.sh

# Set working directory
cd "$(dirname "$0")" || exit

# Activate virtual environment
source venv/bin/activate

# Set Python path
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"

# Run the data collection script
python3 << 'EOF'
import yaml
import pandas as pd
from datetime import datetime
from pathlib import Path
import sys

sys.path.insert(0, 'src')

from data.fetcher import WeatherDataFetcher
from processing.analyzer import WeatherDataProcessor

def main():
    print(f"[{datetime.now()}] Starting weather data pipeline...")
    
    # Load config
    with open('config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Fetch data
    print("Fetching weather data...")
    fetcher = WeatherDataFetcher(config)
    raw_data = fetcher.fetch_all_cities(
        config['cities'],
        config['processing']['forecast_days']
    )
    
    if not raw_data:
        print("ERROR: Failed to fetch data")
        return 1
    
    print(f"Fetched data for {len(raw_data)} cities")
    
    # Process data
    print("Processing data...")
    processor = WeatherDataProcessor(config)
    df = processor.raw_to_dataframe(raw_data)
    df = processor.calculate_moving_averages(df)
    df = processor.detect_anomalies(df)
    
    # Generate alerts
    alerts = processor.generate_alerts(df)
    stats = processor.get_summary_statistics(df)
    
    print(f"Processed {len(df)} records")
    print(f"Detected {stats['anomalies_detected']} anomalies")
    print(f"Generated {len(alerts)} alerts")
    
    # Store results
    output_dir = Path('data')
    output_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Save main data
    df.to_csv(output_dir / f'weather_data_{timestamp}.csv', index=False)
    print(f"Saved data to weather_data_{timestamp}.csv")
    
    # Save alerts
    if alerts:
        alerts_df = pd.DataFrame(alerts)
        alerts_df.to_csv(output_dir / f'alerts_{timestamp}.csv', index=False)
        print(f"Saved {len(alerts)} alerts to alerts_{timestamp}.csv")
        
        # Print critical alerts
        critical = [a for a in alerts if a['type'] in ['high_temperature', 'high_wind']]
        if critical:
            print("\n⚠️  CRITICAL ALERTS:")
            for alert in critical[:5]:
                print(f"  - {alert['city']}: {alert['message']}")
    
    print(f"[{datetime.now()}] Pipeline completed successfully\n")
    return 0

if __name__ == '__main__':
    exit(main())
EOF

# Deactivate virtual environment
deactivate

# Log completion
echo "Cron job completed at $(date)" >> data/cron.log