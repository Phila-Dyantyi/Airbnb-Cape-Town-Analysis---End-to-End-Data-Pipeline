#!/usr/bin/env python
# coding: utf-8

"""
Retrieves:
  1. Airbnb listings for Cape Town (InsideAirbnb dataset)
  2. Meteorological data for Cape Town via Open-Meteo Historical Weather API
"""


# STEP 1: INSTALL DEPENDENCIES

import subprocess
import sys

subprocess.run([sys.executable, "-m", "pip", "install", "requests"])

# STEP 2: IMPORTS


import io
import gzip
import requests
import pandas as pd

from pathlib import Path


# CONFIGURATION


# Cape Town coordinates
LATITUDE  = -33.9388
LONGITUDE = 18.6462

# Weather date range
WEATHER_START = "2025-06-01"
WEATHER_END   = "2025-09-30"

# API URLs
OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"

AIRBNB_URL_JUNE = (
    "https://data.insideairbnb.com/"
    "south-africa/wc/cape-town/2025-06-25/data/listings.csv.gz"
)

AIRBNB_URL_SEP = (
    "https://data.insideairbnb.com/"
    "south-africa/wc/cape-town/2025-09-28/data/listings.csv.gz"
)


# OUTPUT DIRECTORIES


INGEST_DIR = Path(__file__).parent / "Ingest"
INGEST_DIR.mkdir(parents=True, exist_ok=True)

WEATHER_OUTPUT = str(INGEST_DIR / "cape_town_weather_sep2025.csv")
AIRBNB_OUTPUT  = str(INGEST_DIR / "cape_town_airbnb_raw_combined.csv")


# OPEN-METEO VARIABLE MAPPING

VARIABLE_MAP = {
    "time":                       "date",
    "temperature_2m_mean":        "temp_mean_c",
    "temperature_2m_max":         "temp_max_c",
    "temperature_2m_min":         "temp_min_c",
    "daylight_duration":          "daylight_duration_s",
    "sunshine_duration":          "sunshine_duration_s",
    "precipitation_sum":          "precipitation_sum_mm",
    "rain_sum":                   "rain_sum_mm",
    "precipitation_hours":        "precipitation_hours",
    "windspeed_10m_max":          "wind_max_speed_kmh",
    "winddirection_10m_dominant": "wind_dominant_direction_deg",
}


# REQUIRED AIRBNB COLUMNS
# Must match SQL staging table structure

REQUIRED_COLUMNS = [
    "id",
    "last_scraped",
    "name",
    "host_id",
    "host_name",
    "host_since",
    "host_location",
    "host_response_time",
    "host_response_rate",
    "host_acceptance_rate",
    "host_is_superhost",
    "host_listings_count",
    "neighbourhood_cleansed",
    "neighbourhood_group_cleansed",
    "latitude",
    "longitude",
    "property_type",
    "room_type",
    "accommodates",
    "bathrooms_text",
    "bedrooms",
    "beds",
    "price",
    "minimum_nights",
    "maximum_nights",
    "has_availability",
    "availability_365",
    "number_of_reviews",
    "number_of_reviews_ltm",
    "estimated_occupancy_l365d",
    "estimated_revenue_l365d",
    "review_scores_rating",
    "review_scores_accuracy",
    "review_scores_cleanliness",
    "review_scores_checkin",
    "review_scores_communication",
    "review_scores_location",
    "review_scores_value",
    "instant_bookable",
    "reviews_per_month"
]


# WEATHER DATA


def fetch_weather_data(
    lat: float,
    lon: float,
    start: str,
    end: str
) -> pd.DataFrame:

    api_variables = [k for k in VARIABLE_MAP.keys() if k != "time"]

    params = {
        "latitude":           lat,
        "longitude":          lon,
        "start_date":         start,
        "end_date":           end,
        "daily":              ",".join(api_variables),
        "timezone":           "GMT+2",
        "temperature_unit":   "celsius",
        "wind_speed_unit":    "kmh",
        "precipitation_unit": "mm",
        "timeformat":         "iso8601",
    }

    try:
        response = requests.get(
            OPEN_METEO_URL,
            params=params,
            timeout=30
        )

        response.raise_for_status()

        df = pd.DataFrame(response.json().get("daily", {}))

        df.rename(columns=VARIABLE_MAP, inplace=True)

        df["date"] = pd.to_datetime(df["date"])

        df["latitude"] = lat
        df["longitude"] = lon

        return df

    except requests.exceptions.Timeout:
        print("Weather API request timed out")
        return pd.DataFrame()

    except requests.exceptions.ConnectionError:
        print("Failed to connect to Open-Meteo API")
        return pd.DataFrame()

    except Exception as e:
        print(f"Weather fetch failed: {e}")
        return pd.DataFrame()

# AIRBNB DATA

def fetch_airbnb_listings(url: str) -> pd.DataFrame:

    try:
        response = requests.get(
            url,
            timeout=120,
            stream=True
        )

        response.raise_for_status()

        df = pd.read_csv(
            io.BytesIO(gzip.decompress(response.content)),
            low_memory=False
        )

        # Keep only required columns, no value cleaning
        df = df[REQUIRED_COLUMNS]

        return df

    except requests.exceptions.Timeout:
        print("Airbnb download timed out")
        return pd.DataFrame()

    except requests.exceptions.ConnectionError:
        print("Failed to connect to InsideAirbnb")
        return pd.DataFrame()

    except Exception as e:
        print(f"Airbnb fetch failed: {e}")
        return pd.DataFrame()


# MAIN


def main():

    # WEATHER

    weather_df = fetch_weather_data(
        lat=LATITUDE,
        lon=LONGITUDE,
        start=WEATHER_START,
        end=WEATHER_END,
    )

    if not weather_df.empty:
        weather_df.to_csv(
            WEATHER_OUTPUT,
            index=False,
            encoding="utf-8"
        )

    # AIRBNB — JUNE

    june_df = fetch_airbnb_listings(AIRBNB_URL_JUNE)

    # AIRBNB — SEPTEMBER

    sep_df = fetch_airbnb_listings(AIRBNB_URL_SEP)

    # COMBINE

    combined_df = pd.concat(
        [june_df, sep_df],
        ignore_index=True
    )

    # SAVE

    combined_df.to_csv(
        AIRBNB_OUTPUT,
        index=False,
        encoding="utf-8"
    )

# EXECUTE

if __name__ == "__main__":
    main()
