"""
Tests for weather data fetcher module.
"""

import pytest
from unittest.mock import Mock, patch
from src.data.fetcher import WeatherDataFetcher


@pytest.fixture
def sample_config():
    """Sample configuration for testing."""
    return {
        "api": {
            "base_url": "https://api.open-meteo.com/v1/forecast",
            "timeout": 30,
            "retry_attempts": 3,
        },
        "weather_params": ["temperature_2m", "relative_humidity_2m", "precipitation"],
    }


@pytest.fixture
def sample_city():
    """Sample city data for testing."""
    return {"name": "Test City", "lat": -23.5505, "lon": -46.6333}


@pytest.fixture
def sample_api_response():
    """Sample API response."""
    return {
        "hourly": {
            "time": ["2024-01-01T00:00", "2024-01-01T01:00"],
            "temperature_2m": [25.5, 26.0],
            "relative_humidity_2m": [65, 68],
            "precipitation": [0, 0.5],
        }
    }


class TestWeatherDataFetcher:
    """Test cases for WeatherDataFetcher class."""

    def test_initialization(self, sample_config):
        """Test fetcher initialization."""
        fetcher = WeatherDataFetcher(sample_config)

        assert fetcher.base_url == sample_config["api"]["base_url"]
        assert fetcher.timeout == sample_config["api"]["timeout"]
        assert fetcher.retry_attempts == sample_config["api"]["retry_attempts"]
        assert fetcher.weather_params == sample_config["weather_params"]

    @patch("src.data.fetcher.requests.get")
    def test_fetch_city_weather_success(
        self, mock_get, sample_config, sample_city, sample_api_response
    ):
        """Test successful weather data fetch."""
        mock_response = Mock()
        mock_response.json.return_value = sample_api_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        fetcher = WeatherDataFetcher(sample_config)
        result = fetcher.fetch_city_weather(
            lat=sample_city["lat"],
            lon=sample_city["lon"],
            city_name=sample_city["name"],
        )

        assert result is not None
        assert result["city_name"] == sample_city["name"]
        assert result["lat"] == sample_city["lat"]
        assert result["lon"] == sample_city["lon"]
        assert "fetch_timestamp" in result
        assert "hourly" in result

    @patch("src.data.fetcher.requests.get")
    @patch("src.data.fetcher.time.sleep")  # Mock sleep para não esperar
    def test_fetch_city_weather_retry_on_failure(
        self,
        mock_sleep,  # Adicione este parâmetro
        mock_get,
        sample_config,
        sample_city,
    ):
        """Test retry logic on API failure."""
        mock_get.side_effect = Exception("API Error")

        fetcher = WeatherDataFetcher(sample_config)
        result = fetcher.fetch_city_weather(
            lat=sample_city["lat"],
            lon=sample_city["lon"],
            city_name=sample_city["name"],
        )

        assert result is None
        assert mock_get.call_count == sample_config["api"]["retry_attempts"]
        # Verificar que tentou dormir (retry logic)
        assert mock_sleep.call_count == sample_config["api"]["retry_attempts"] - 1

    @patch("src.data.fetcher.requests.get")
    @patch("src.data.fetcher.time.sleep")
    def test_fetch_all_cities(
        self, mock_sleep, mock_get, sample_config, sample_api_response
    ):
        """Test fetching data for multiple cities."""

        # Criar respostas diferentes para cada cidade
        def create_response(city_name):
            response = Mock()
            api_data = sample_api_response.copy()
            api_data["city_name"] = city_name  # Atualizar com nome correto
            response.json.return_value = api_data
            response.raise_for_status.return_value = None
            return response

        cities = [
            {"name": "City1", "lat": -23.5, "lon": -46.6},
            {"name": "City2", "lat": -22.9, "lon": -43.2},
        ]

        # Mock retorna respostas diferentes
        mock_get.side_effect = [create_response("City1"), create_response("City2")]

        fetcher = WeatherDataFetcher(sample_config)
        results = fetcher.fetch_all_cities(cities)

        assert len(results) == 2
        assert results[0]["city_name"] == "City1"
        assert results[1]["city_name"] == "City2"
