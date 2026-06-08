"""Tests for merge_all_etfs from depot_risk_assessment/merge.py."""

from unittest.mock import patch

import pandas as pd
import pytest

from depot_risk_assessment.etf_handling import ETFConfig, ETFHandler, TickerConfig
from depot_risk_assessment.merge import merge_all_etfs


def _make_ticker_config(editor: str) -> TickerConfig:
    return TickerConfig(wkn="DUMMY", editor=editor, url=None, file_name="dummy")


def _make_amundi_config() -> ETFConfig:
    zusammensetzung = pd.DataFrame(
        {
            "ISIN": ["US0378331005", "US5949181045"],
            "Name": ["Apple Inc", "Microsoft Corp"],
            "Sektor": [None, None],
            "Standort": [None, None],
            "Gewichtung": [60.0, 40.0],
            "Wert": [600.0, 400.0],
        }
    )
    return ETFConfig(
        ticker_config=_make_ticker_config("amundi"),
        zusammensetzung=zusammensetzung,
        total_value=1000.0,
    )


def _make_ishares_config() -> ETFConfig:
    zusammensetzung = pd.DataFrame(
        {
            "Emittententicker": ["NVDA", "TSM"],
            "Name": ["Nvidia Corp", "Taiwan Semiconductor"],
            "Sektor": ["Technology", "Technology"],
            "Standort": ["United States", "Taiwan"],
            "Gewichtung": [70.0, 30.0],
            "Wert": [700.0, 300.0],
        }
    )
    return ETFConfig(
        ticker_config=_make_ticker_config("iShares"),
        zusammensetzung=zusammensetzung,
        total_value=1000.0,
    )


@pytest.fixture
def etf_handler():
    return ETFHandler(etfs=[_make_amundi_config(), _make_ishares_config()])


@pytest.fixture
def ex_isin_info():
    """Pre-populated ISIN cache so that get_infos_from_yahoo returns nothing.

    Standort must be non-null so that after prepare_data_by_isin the column
    stays as object dtype (all-NaN would collapse to float64, causing a type
    mismatch when merging with the iShares ticker-keyed DataFrame).
    """
    return pd.DataFrame(
        {
            "ISIN": ["US0378331005", "US5949181045"],
            "Symbol": ["AAPL", "MSFT"],
            "Sektor": ["Technology", "Technology"],
            "Standort": ["United States", "United States"],
            "Name": ["Apple Inc", "Microsoft Corp"],
        }
    )


class TestMergeAllEtfs:
    def test_returns_tuple_of_two_dataframes(self, etf_handler, ex_isin_info):
        with patch("depot_risk_assessment.merge.get_infos_from_yahoo") as mock_yahoo:
            mock_yahoo.return_value = pd.DataFrame()
            result = merge_all_etfs(etf_handler, ex_isin_info)
        assert isinstance(result, tuple)
        assert len(result) == 2
        merged_df, updated_isin_info = result
        assert isinstance(merged_df, pd.DataFrame)
        assert isinstance(updated_isin_info, pd.DataFrame)

    def test_merged_df_has_wert_column(self, etf_handler, ex_isin_info):
        with patch("depot_risk_assessment.merge.get_infos_from_yahoo") as mock_yahoo:
            mock_yahoo.return_value = pd.DataFrame()
            merged_df, _ = merge_all_etfs(etf_handler, ex_isin_info)
        assert "Wert" in merged_df.columns

    def test_wert_sum_close_to_combined_total_values(self, etf_handler, ex_isin_info):
        expected_total = sum(etf.total_value for etf in etf_handler.etfs)
        with patch("depot_risk_assessment.merge.get_infos_from_yahoo") as mock_yahoo:
            mock_yahoo.return_value = pd.DataFrame()
            merged_df, _ = merge_all_etfs(etf_handler, ex_isin_info)
        assert abs(merged_df["Wert"].sum() - expected_total) < 2.0

    def test_merged_df_is_not_empty(self, etf_handler, ex_isin_info):
        with patch("depot_risk_assessment.merge.get_infos_from_yahoo") as mock_yahoo:
            mock_yahoo.return_value = pd.DataFrame()
            merged_df, _ = merge_all_etfs(etf_handler, ex_isin_info)
        assert len(merged_df) > 0
