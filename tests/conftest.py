import pandas as pd
import pytest


@pytest.fixture
def sector_mapping():
    return {"Technology": "Technologie", "Healthcare": "Gesundheit", "Energy": "Energie"}


@pytest.fixture
def minimal_holdings_df():
    return pd.DataFrame(
        {
            "Name": ["Apple Inc", "Microsoft Corp"],
            "ISIN": ["US0378331005", "US5949181045"],
            "Sektor": ["Technologie", "Technologie"],
            "Standort": ["United States", "United States"],
            "Gewichtung": [60.0, 40.0],
            "Wert": [600.0, 400.0],
        }
    )
