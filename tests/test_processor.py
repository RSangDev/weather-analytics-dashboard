"""
Tests for weather data processor module.
"""

import pytest
import pandas as pd
import numpy as np
from src.processing.analyzer import WeatherDataProcessor


@pytest.fixture
def sample_config():
    """Sample configuration for testing."""
    return {
        'processing': {
            'moving_average_window': 3,
            'anomaly_threshold': 2.0
        },
        'alerts': {
            'temperature': {
                'high_threshold': 35,
                'low_threshold': 5
            },
            'wind_speed': {
                'high_threshold': 60
            },
            'precipitation': {
                'high_threshold': 50
            }
        }
    }


@pytest.fixture
def sample_raw_data():
    """Sample raw API data."""
    return [
        {
            'city_name': 'Test City',
            'lat': -23.5,
            'lon': -46.6,
            'hourly': {
                'time': ['2024-01-01T00:00', '2024-01-01T01:00', '2024-01-01T02:00'],
                'temperature_2m': [25.0, 26.0, 27.0],
                'relative_humidity_2m': [65, 68, 70],
                'precipitation': [0, 0.5, 1.0],
                'wind_speed_10m': [10, 15, 20],
                'cloud_cover': [30, 40, 50],
                'pressure_msl': [1013, 1014, 1015]
            }
        }
    ]


@pytest.fixture
def sample_dataframe():
    """Sample processed DataFrame."""
    return pd.DataFrame({
        'time': pd.to_datetime(['2024-01-01T00:00', '2024-01-01T01:00', '2024-01-01T02:00']),
        'city': ['City1', 'City1', 'City1'],
        'temperature_2m': [20.0, 25.0, 30.0],
        'relative_humidity_2m': [60, 65, 70],
        'precipitation': [0, 1, 2],
        'wind_speed_10m': [10, 15, 20],
        'cloud_cover': [30, 40, 50],
        'latitude': [-23.5, -23.5, -23.5],
        'longitude': [-46.6, -46.6, -46.6]
    })


class TestWeatherDataProcessor:
    """Test cases for WeatherDataProcessor class."""
    
    def test_initialization(self, sample_config):
        """Test processor initialization."""
        processor = WeatherDataProcessor(sample_config)
        
        assert processor.ma_window == 3
        assert processor.anomaly_threshold == 2.0
        assert processor.alert_config == sample_config['alerts']
    
    def test_raw_to_dataframe(self, sample_config, sample_raw_data):
        """Test conversion from raw data to DataFrame."""
        processor = WeatherDataProcessor(sample_config)
        df = processor.raw_to_dataframe(sample_raw_data)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert 'city' in df.columns
        assert 'temperature_2m' in df.columns
        assert df['city'].iloc[0] == 'Test City'
    
    def test_calculate_moving_averages(self, sample_config, sample_dataframe):
        """Test moving average calculation."""
        processor = WeatherDataProcessor(sample_config)
        df = processor.calculate_moving_averages(sample_dataframe)
        
        assert 'temp_ma' in df.columns
        assert not df['temp_ma'].isna().all()
    
    def test_detect_anomalies(self, sample_config, sample_dataframe):
        """Test anomaly detection."""
        # Criar DataFrame com mais valores para melhor detecção estatística
        df = pd.DataFrame({
            'time': pd.to_datetime([
                '2024-01-01T00:00', '2024-01-01T01:00', '2024-01-01T02:00',
                '2024-01-01T03:00', '2024-01-01T04:00', '2024-01-01T05:00',
                '2024-01-01T06:00', '2024-01-01T07:00'
            ]),
            'city': ['City1'] * 8,
            'temperature_2m': [20.0, 21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 100.0],
            'relative_humidity_2m': [60, 61, 62, 63, 64, 65, 66, 67],
            'precipitation': [0, 0, 0, 0, 0, 0, 0, 0],
            'wind_speed_10m': [10, 11, 12, 13, 14, 15, 16, 17],
            'cloud_cover': [30, 31, 32, 33, 34, 35, 36, 37],
            'latitude': [-23.5] * 8,
            'longitude': [-46.6] * 8
        })

        processor = WeatherDataProcessor(sample_config)
        df = processor.detect_anomalies(df)

        assert 'temp_anomaly' in df.columns
        assert df['temp_anomaly'].any()  # Deve detectar pelo menos uma anomalia
        # Verificar que a temperatura extrema (100°C) foi detectada
        assert df.loc[df['temperature_2m'] == 100.0, 'temp_anomaly'].iloc[0] == True
    
    def test_generate_alerts_high_temperature(self, sample_config):
        """Test high temperature alert generation."""
        df = pd.DataFrame({
            'time': pd.to_datetime(['2024-01-01T00:00']),
            'city': ['Test City'],
            'temperature_2m': [40.0],  # Above threshold
            'relative_humidity_2m': [60],
            'precipitation': [0],
            'wind_speed_10m': [10]
        })
        
        processor = WeatherDataProcessor(sample_config)
        alerts = processor.generate_alerts(df)
        
        assert len(alerts) > 0
        assert alerts[0]['type'] == 'high_temperature'
        assert alerts[0]['city'] == 'Test City'
    
    def test_generate_alerts_high_wind(self, sample_config):
        """Test high wind speed alert generation."""
        df = pd.DataFrame({
            'time': pd.to_datetime(['2024-01-01T00:00']),
            'city': ['Test City'],
            'temperature_2m': [25.0],
            'relative_humidity_2m': [60],
            'precipitation': [0],
            'wind_speed_10m': [65.0]  # Above threshold
        })
        
        processor = WeatherDataProcessor(sample_config)
        alerts = processor.generate_alerts(df)
        
        assert len(alerts) > 0
        assert any(alert['type'] == 'high_wind' for alert in alerts)
    
    def test_aggregate_daily_stats(self, sample_config, sample_dataframe):
        """Test daily statistics aggregation."""
        processor = WeatherDataProcessor(sample_config)
        daily_df = processor.aggregate_daily_stats(sample_dataframe)
        
        assert isinstance(daily_df, pd.DataFrame)
        assert 'date' in daily_df.columns
        assert 'temp_mean' in daily_df.columns
        assert 'temp_min' in daily_df.columns
        assert 'temp_max' in daily_df.columns
    
    def test_get_summary_statistics(self, sample_config, sample_dataframe):
        """Test summary statistics calculation."""
        processor = WeatherDataProcessor(sample_config)
        stats = processor.get_summary_statistics(sample_dataframe)
        
        assert 'total_cities' in stats
        assert 'total_records' in stats
        assert 'avg_temperature' in stats
        assert stats['total_cities'] == 1
        assert stats['total_records'] == 3
    
    def test_empty_dataframe_handling(self, sample_config):
        """Test handling of empty DataFrame."""
        processor = WeatherDataProcessor(sample_config)
        empty_df = pd.DataFrame()
        
        # Should not raise exceptions
        stats = processor.get_summary_statistics(empty_df)
        assert isinstance(stats, dict)