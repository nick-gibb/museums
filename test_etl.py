import pandas as pd
import pytest


from etl import load_from_cache, extract_from_wikipedia


def test_load_from_cache():
    # Test cache file exists
    cache_name = "test_cache.pkl"

    def create_cache_file():
        df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        df.to_pickle(cache_name)

    create_cache_file()
    df = load_from_cache()
    assert isinstance(df, pd.DataFrame)

    # Test cache file does not exist
    import os

    os.remove(cache_name)
    with pytest.raises(Exception) as e:
        load_from_cache(cache_name)
    assert str(e.value) == "Cache file not found"


def test_extract_from_wikipedia():
    df = extract_from_wikipedia()

    # Assert that the returned object is a DataFrame
    assert isinstance(df, pd.DataFrame)

    # Assert that the DataFrame has the expected columns
    assert all(
        col in df.columns
        for col in ["Name", "Visitors per year", "Year reported", "Country flag, city"]
    )
