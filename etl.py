"""
Use pandas to extract data from Wikipedia, transform it, and load it into a sqlite database.

https://en.wikipedia.org/wiki/List_of_most_visited_museums

Usage:
    python etl.py --clear
    python etl.py --use-cache
    python etl.py

The --clear flag will drop the tables if they exist, and the --use-cache flag will use the cache file to reduce API calls.
"""

import argparse
import logging
import os
import sqlite3
import sys

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)

# API key for api.api-ninjas.com
# This is a free API that returns the population of a city
API_KEY = os.getenv("API_KEY")
if not API_KEY:
    logger.error("API_KEY is not set.")
    sys.exit(1)

parser = argparse.ArgumentParser()
parser.add_argument("--clear", help="Clear the database", action="store_true")
# Accept cache file as an optional argument
parser.add_argument(
    "--use-cache",
    help="Use the cache file to reduce API calls (for development)",
    action="store_true",
)


def extract_from_wikipedia():
    """
    Extract data from Wikipedia and return a pandas dataframe.
    """
    # TODO(Nick): We should remove the pandas dependency and use requests instead.
    # Dataframes are slow and not necessary for this use case.
    df = pd.read_html("https://en.wikipedia.org/wiki/List_of_most_visited_museums")[0]
    return df


def get_city_population(city: str) -> int:
    """
    Get the population of a city from an API.
    Returns the population as int, or NaN if city not found.

    Parameters:
        city (str): The name of the city.

    Returns:
        int: The population of the city.
        np.nan: If the city is not found.
    """
    api_url = "https://api.api-ninjas.com/v1/city?name={}".format(city)
    response = requests.get(api_url, headers={"X-Api-Key": API_KEY})
    if response.status_code == requests.codes.ok:
        # Return the population of the city as an integer
        try:
            return int(response.json()[0]["population"])
        except:  # city not found
            return np.nan
    else:
        logger.error("Error:", response.status_code, response.text)
        return np.nan


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Some data cleaning and transformation.

    Args:
        df: A pandas dataframe with the data from Wikipedia.

    Returns:
        A pandas dataframe with the transformed data, ready to be loaded into the database.
    """
    df.rename(columns={"Country flag, city": "City"}, inplace=True)
    df.dropna(subset=["Visitors per year"], inplace=True)
    logger.info("Querying the API for city populations...")
    df["City population"] = df["City"].apply(get_city_population)
    # Drop rows where the population is unknown
    df.dropna(subset=["City population"], inplace=True)
    # Replace square brackets and the text inside with an empty string.
    # Also strip the commas from the numbers and the whitespace.
    df["Visitors per year"] = (
        df["Visitors per year"]
        .str.replace(r"\[.*\]", "", regex=True)
        .str.replace(",", "")
        .str.strip()
    )
    df["Visitors per year"] = df["Visitors per year"].astype(int)
    # Drop the Year reported column
    df["Year reported"] = df["Year reported"].str[:4]
    # Rename all columns to lowercase and replace spaces with underscores
    df.columns = df.columns.str.lower().str.replace(" ", "_")


def load_cities(df: pd.DataFrame) -> None:
    """
    Load the city data into the database.

    Args:
        df: A pandas dataframe with the data from Wikipedia, transformed.

    Returns:
        None
    """
    conn = sqlite3.connect("museums.db")
    # Rename the columns to match the table, and drop duplicates (there are multiple rows for each city)
    cities = (
        df[["city_population", "city"]]
        .rename(columns={"city_population": "population", "city": "name"})
        .drop_duplicates()
    )
    # Insert the data into the database, or update the population if the city name already exists
    cities.to_sql("city", conn, if_exists="replace", index=False)
    logger.info(f"Loaded {len(cities)} cities into the database.")
    conn.close()


def load_museums(museums: pd.DataFrame) -> None:
    """
    Load the museum data into the database.

    Args:
        df: A pandas dataframe with the data from Wikipedia, transformed.

    Returns:
        None
    """
    conn = sqlite3.connect("museums.db")
    # Rename the columns to match the table
    museums = museums.rename(
        columns={
            "visitors per year": "visitors_per_year",
            "year reported": "year_reported",
        }
    )
    # Select the columns to insert
    museums = museums[["name", "city", "visitors_per_year", "year_reported"]]
    # If a museum name (our PK) already exists, update the other columns
    museums.to_sql("museum", conn, if_exists="replace", index=False)
    logger.info(f"Loaded {len(museums)} museums into the database.")


def initialize_db(clear: bool = False) -> None:
    """
    Create the database and tables if they don't exist.

    Args:
        clear: If True, drop the tables if they exist.

    Returns:
        None
    """
    # TODO(Nick): We should normalize the schema further
    # New table: museum_visits(museum_name, visitors_per_year, year_reported)
    # Foreign key: museum_visits.museum_name -> museum.name

    # TODO(Nick): We should have an id column for each table and use that as the primary key
    # Currently we are using the name column as the primary key

    conn = sqlite3.connect("museums.db")
    c = conn.cursor()
    if clear:
        logger.warning("Dropping tables if they exist.")
        c.execute("DROP TABLE IF EXISTS museum;")
        c.execute("DROP TABLE IF EXISTS city;")
    city_table_exists = c.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='city';"
    ).fetchone()
    museums_table_exists = c.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='museum';"
    ).fetchone()

    # Create tables if they don't exist
    if not city_table_exists:
        c.execute(
            """
        CREATE TABLE city (
            name TEXT PRIMARY KEY NOT NULL,
            population INTEGER NOT NULL
        );
        """
        )
        logger.info("Created city table.")
    if not museums_table_exists:
        c.execute(
            """
        CREATE TABLE museum (
            name TEXT PRIMARY KEY NOT NULL,
            city_name TEXT REFERENCES city(name) NOT NULL,
            visitors_per_year INTEGER NOT NULL,
            year_reported INTEGER
        );
        """
        )
        logger.info("Created museum table.")
    # Commit the changes to the database
    conn.commit()
    # Close the connection to the database
    conn.close()


def load_from_cache(cache_name="cache.pkl") -> pd.DataFrame:
    """
    Load the data from the cache file to reduce API calls during development.

    Returns:
        pd.DataFrame: Transformed data.

    Raises:
        Exception: If the cache file is not found.
    """
    try:
        df = pd.read_pickle(cache_name)
    except FileNotFoundError:
        raise Exception("Cache file not found")
    return df


if __name__ == "__main__":
    args = parser.parse_args()
    initialize_db(args.clear)
    df = extract_from_wikipedia()
    if args.use_cache:  # For development, to reduce the number of API calls
        df = load_from_cache()
    else:  # For production
        try:
            transform_data(df)
        except Exception as e:
            logger.error("Error transforming data:", e)
            sys.exit(1)
        df.to_pickle("cache.pkl")  # For development
    load_cities(df)
    load_museums(df)
    logger.info("Done.")
