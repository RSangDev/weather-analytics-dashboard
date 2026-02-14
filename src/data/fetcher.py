"""
Weather data fetcher module.
Handles API requests to Open-Meteo with retry logic and error handling.
"""

import requests
from typing import Dict, List, Optional
import time
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WeatherDataFetcher:
    """Fetches weather data from Open-Meteo API."""

    def __init__(self, config: Dict):
        """
        Initialize the weather data fetcher.

        Args:
            config: Configuration dictionary containing API settings
        """
        self.base_url = config["api"]["base_url"]
        self.timeout = config["api"]["timeout"]
        self.retry_attempts = config["api"]["retry_attempts"]
        self.weather_params = config["weather_params"]

    def fetch_city_weather(
        self, lat: float, lon: float, city_name: str, forecast_days: int = 7
    ) -> Optional[Dict]:
        """
        Fetch weather data for a specific city.
        """
        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": ",".join(self.weather_params),
            "forecast_days": forecast_days,
            "timezone": "America/Sao_Paulo",
        }

        for attempt in range(self.retry_attempts):
            try:
                response = requests.get(
                    self.base_url, params=params, timeout=self.timeout
                )
                response.raise_for_status()

                data = response.json()
                data["city_name"] = city_name
                data["lat"] = lat
                data["lon"] = lon
                data["fetch_timestamp"] = datetime.now().isoformat()

                logger.info(f"Successfully fetched data for {city_name}")
                return data

            except Exception as e:  # ✅ CAPTURA QUALQUER EXCEÇÃO
                logger.warning(
                    f"Attempt {attempt + 1}/{self.retry_attempts} failed for {city_name}: {e}"  # noqa 
                )
                if attempt < self.retry_attempts - 1:
                    time.sleep(2**attempt)  # Exponential backoff
                else:
                    logger.error(
                        f"Failed to fetch data for {city_name} after all attempts"
                    )
                    return None
            except Exception as e:  # ✅ ADICIONE ESTE BLOCO
                logger.warning(
                    f"Attempt {attempt + 1}/{self.retry_attempts} failed for {city_name}: {e}"  # noqa 
                )
                if attempt < self.retry_attempts - 1:
                    time.sleep(2**attempt)
                else:
                    logger.error(
                        f"Failed to fetch data for {city_name} after all attempts"
                    )
                    return None

    def fetch_all_cities(
        self, cities: List[Dict], forecast_days: int = 7
    ) -> List[Dict]:
        """
        Fetch weather data for all cities.

        Args:
            cities: List of city dictionaries with lat, lon, and name
            forecast_days: Number of days to forecast

        Returns:
            List of weather data dictionaries
        """
        all_data = []

        for city in cities:
            data = self.fetch_city_weather(
                lat=city["lat"],
                lon=city["lon"],
                city_name=city["name"],
                forecast_days=forecast_days,
            )

            if data:
                all_data.append(data)

            # Rate limiting - be nice to the API
            time.sleep(0.5)

        logger.info(f"Fetched data for {len(all_data)}/{len(cities)} cities")
        return all_data
