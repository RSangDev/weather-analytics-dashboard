"""
Weather data processing module - VERSÃO COM ANÁLISE REGIONAL
Handles data transformation, analysis, anomaly detection, and regional insights.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WeatherDataProcessor:
    """Processes and analyzes weather data with regional insights."""
    
    def __init__(self, config: Dict):
        """
        Initialize the weather data processor.
        
        Args:
            config: Configuration dictionary
        """
        self.ma_window = config['processing']['moving_average_window']
        self.anomaly_threshold = config['processing']['anomaly_threshold']
        self.alert_config = config['alerts']
    
    def raw_to_dataframe(self, raw_data: List[Dict]) -> pd.DataFrame:
        """
        Convert raw API data to pandas DataFrame.
        
        Args:
            raw_data: List of raw weather data dictionaries
            
        Returns:
            Processed DataFrame
        """
        all_records = []
        
        for city_data in raw_data:
            city_name = city_data['city_name']
            lat = city_data['lat']
            lon = city_data['lon']
            hourly_data = city_data['hourly']
            
            # Convert to DataFrame
            df = pd.DataFrame(hourly_data)
            df['time'] = pd.to_datetime(df['time'])
            df['city'] = city_name
            df['latitude'] = lat
            df['longitude'] = lon
            
            # Adicionar informações extras se disponíveis
            if 'state' in city_data:
                df['state'] = city_data['state']
            if 'region' in city_data:
                df['region'] = city_data['region']
            
            all_records.append(df)
        
        if not all_records:
            return pd.DataFrame()
        
        combined_df = pd.concat(all_records, ignore_index=True)
        logger.info(f"Created DataFrame with {len(combined_df)} records from {combined_df['city'].nunique()} cities")
        
        return combined_df
    
    def calculate_moving_averages(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate moving averages for temperature.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with moving average columns
        """
        df = df.copy()
        
        for city in df['city'].unique():
            city_mask = df['city'] == city
            df.loc[city_mask, 'temp_ma'] = df.loc[city_mask, 'temperature_2m'].rolling(
                window=self.ma_window, min_periods=1
            ).mean()
        
        return df
    
    def detect_anomalies(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Detect temperature anomalies using standard deviation.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with anomaly flags
        """
        df = df.copy()
        df['temp_anomaly'] = False
        
        for city in df['city'].unique():
            city_mask = df['city'] == city
            city_temps = df.loc[city_mask, 'temperature_2m']
            
            mean_temp = city_temps.mean()
            std_temp = city_temps.std()
            
            if std_temp > 0:
                anomaly_mask = np.abs(city_temps - mean_temp) > (self.anomaly_threshold * std_temp)
                df.loc[city_mask, 'temp_anomaly'] = anomaly_mask
        
        anomaly_count = df['temp_anomaly'].sum()
        logger.info(f"Detected {anomaly_count} temperature anomalies")
        
        return df
    
    def generate_alerts(self, df: pd.DataFrame) -> List[Dict]:
        """
        Generate weather alerts based on thresholds.
        
        Args:
            df: Input DataFrame
            
        Returns:
            List of alert dictionaries
        """
        alerts = []
        
        # Temperature alerts
        high_temp_mask = df['temperature_2m'] > self.alert_config['temperature']['high_threshold']
        low_temp_mask = df['temperature_2m'] < self.alert_config['temperature']['low_threshold']
        
        for idx, row in df[high_temp_mask].iterrows():
            alerts.append({
                'type': 'high_temperature',
                'city': row['city'],
                'time': row['time'],
                'value': row['temperature_2m'],
                'message': f"High temperature alert: {row['temperature_2m']:.1f}°C"
            })
        
        for idx, row in df[low_temp_mask].iterrows():
            alerts.append({
                'type': 'low_temperature',
                'city': row['city'],
                'time': row['time'],
                'value': row['temperature_2m'],
                'message': f"Low temperature alert: {row['temperature_2m']:.1f}°C"
            })
        
        # Wind speed alerts
        high_wind_mask = df['wind_speed_10m'] > self.alert_config['wind_speed']['high_threshold']
        for idx, row in df[high_wind_mask].iterrows():
            alerts.append({
                'type': 'high_wind',
                'city': row['city'],
                'time': row['time'],
                'value': row['wind_speed_10m'],
                'message': f"High wind alert: {row['wind_speed_10m']:.1f} km/h"
            })
        
        # Precipitation alerts
        high_precip_mask = df['precipitation'] > self.alert_config['precipitation']['high_threshold']
        for idx, row in df[high_precip_mask].iterrows():
            alerts.append({
                'type': 'heavy_precipitation',
                'city': row['city'],
                'time': row['time'],
                'value': row['precipitation'],
                'message': f"Heavy precipitation alert: {row['precipitation']:.1f} mm"
            })
        
        logger.info(f"Generated {len(alerts)} weather alerts")
        return alerts
    
    def aggregate_daily_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate hourly data to daily statistics.
        
        Args:
            df: Input DataFrame with hourly data
            
        Returns:
            DataFrame with daily aggregated statistics
        """
        df = df.copy()
        df['date'] = df['time'].dt.date
        
        # Colunas base para agregação
        group_cols = ['city', 'date']
        agg_dict = {
            'temperature_2m': ['mean', 'min', 'max'],
            'relative_humidity_2m': 'mean',
            'precipitation': 'sum',
            'wind_speed_10m': 'max',
            'cloud_cover': 'mean',
            'latitude': 'first',
            'longitude': 'first'
        }
        
        # Adicionar campos regionais se existirem
        if 'state' in df.columns:
            agg_dict['state'] = 'first'
        if 'region' in df.columns:
            agg_dict['region'] = 'first'
        
        daily_stats = df.groupby(group_cols).agg(agg_dict).reset_index()
        
        # Flatten column names
        daily_stats.columns = [
            'city', 'date', 'temp_mean', 'temp_min', 'temp_max',
            'humidity_mean', 'precipitation_total', 'wind_max',
            'cloud_cover_mean', 'latitude', 'longitude'
        ]
        
        # Adicionar colunas regionais de volta
        if 'state' in df.columns:
            state_map = df.groupby('city')['state'].first()
            daily_stats['state'] = daily_stats['city'].map(state_map)
        
        if 'region' in df.columns:
            region_map = df.groupby('city')['region'].first()
            daily_stats['region'] = daily_stats['city'].map(region_map)
        
        return daily_stats
    
    def get_summary_statistics(self, df: pd.DataFrame) -> Dict:
        """
        Calculate summary statistics across all cities.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Dictionary with summary statistics
        """
        if df.empty:
            return {
                'total_cities': 0,
                'total_records': 0,
                'avg_temperature': 0.0,
                'max_temperature': 0.0,
                'min_temperature': 0.0,
                'avg_humidity': 0.0,
                'total_precipitation': 0.0,
                'max_wind_speed': 0.0,
                'anomalies_detected': 0,
            }
        
        stats = {
            'total_cities': df['city'].nunique(),
            'total_records': len(df),
            'avg_temperature': df['temperature_2m'].mean(),
            'max_temperature': df['temperature_2m'].max(),
            'min_temperature': df['temperature_2m'].min(),
            'avg_humidity': df['relative_humidity_2m'].mean(),
            'total_precipitation': df['precipitation'].sum(),
            'max_wind_speed': df['wind_speed_10m'].max(),
            'anomalies_detected': df.get('temp_anomaly', pd.Series([False])).sum(),
        }
        
        return stats
    
    def get_regional_statistics(self, df: pd.DataFrame) -> Dict:
        """
        Calculate statistics by region.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Dictionary with regional statistics
        """
        if df.empty or 'region' not in df.columns:
            return {}
        
        regional_stats = {}
        
        for region in df['region'].unique():
            region_df = df[df['region'] == region]
            
            regional_stats[region] = {
                'cities': region_df['city'].nunique(),
                'avg_temperature': region_df['temperature_2m'].mean(),
                'max_temperature': region_df['temperature_2m'].max(),
                'min_temperature': region_df['temperature_2m'].min(),
                'avg_humidity': region_df['relative_humidity_2m'].mean(),
                'total_precipitation': region_df['precipitation'].sum(),
                'max_wind_speed': region_df['wind_speed_10m'].max(),
            }
        
        logger.info(f"Calculated statistics for {len(regional_stats)} regions")
        return regional_stats
    
    def get_city_rankings(self, daily_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Get rankings of cities by different metrics.
        
        Args:
            daily_df: Daily aggregated DataFrame
            
        Returns:
            Dictionary with different rankings
        """
        if daily_df.empty:
            return {}
        
        # Pegar dados mais recentes
        latest = daily_df.groupby('city').last().reset_index()
        
        rankings = {
            'hottest': latest.nlargest(10, 'temp_max')[['city', 'temp_max']].reset_index(drop=True),
            'coldest': latest.nsmallest(10, 'temp_min')[['city', 'temp_min']].reset_index(drop=True),
            'rainiest': latest.nlargest(10, 'precipitation_total')[['city', 'precipitation_total']].reset_index(drop=True),
            'windiest': latest.nlargest(10, 'wind_max')[['city', 'wind_max']].reset_index(drop=True),
            'most_humid': latest.nlargest(10, 'humidity_mean')[['city', 'humidity_mean']].reset_index(drop=True),
        }
        
        return rankings
    
    def detect_weather_patterns(self, df: pd.DataFrame) -> Dict:
        """
        Detect weather patterns like cold fronts or heat waves.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Dictionary with detected patterns
        """
        patterns = {
            'heat_waves': [],
            'cold_fronts': [],
            'heavy_rain_events': []
        }
        
        if df.empty:
            return patterns
        
        # Detectar ondas de calor (temperatura > 35°C por 3+ horas consecutivas)
        for city in df['city'].unique():
            city_df = df[df['city'] == city].sort_values('time')
            city_df['heat_wave'] = city_df['temperature_2m'] > 35
            
            # Contar sequências consecutivas
            city_df['heat_group'] = (city_df['heat_wave'] != city_df['heat_wave'].shift()).cumsum()
            heat_sequences = city_df[city_df['heat_wave']].groupby('heat_group').size()
            
            if (heat_sequences >= 3).any():
                patterns['heat_waves'].append({
                    'city': city,
                    'duration_hours': heat_sequences.max(),
                    'max_temp': city_df[city_df['heat_wave']]['temperature_2m'].max()
                })
        
        # Detectar frentes frias (queda de temperatura > 10°C em 24h)
        for city in df['city'].unique():
            city_df = df[df['city'] == city].sort_values('time')
            city_df['temp_change_24h'] = city_df['temperature_2m'].diff(periods=24)
            
            cold_front = city_df[city_df['temp_change_24h'] < -10]
            if not cold_front.empty:
                patterns['cold_fronts'].append({
                    'city': city,
                    'temp_drop': cold_front['temp_change_24h'].min(),
                    'time': cold_front['time'].iloc[0]
                })
        
        # Detectar eventos de chuva intensa (> 30mm em 3 horas)
        for city in df['city'].unique():
            city_df = df[df['city'] == city].sort_values('time')
            city_df['precip_3h'] = city_df['precipitation'].rolling(window=3).sum()
            
            heavy_rain = city_df[city_df['precip_3h'] > 30]
            if not heavy_rain.empty:
                patterns['heavy_rain_events'].append({
                    'city': city,
                    'precipitation_3h': heavy_rain['precip_3h'].max(),
                    'time': heavy_rain['time'].iloc[0]
                })
        
        return patterns