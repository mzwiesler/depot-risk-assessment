import pandas as pd

from depot_risk_assessment.dataframe_ops import (
    merge_and_drop_col,
    prepare_single_type,
    rescale,
    sum_and_replace,
    sum_duplicates_by,
)


class TestRescale:
    def test_normal_case_sums_to_100(self):
        col = pd.Series([10.0, 20.0, 30.0, 40.0])
        result = rescale(col)
        assert abs(result.sum() - 100.0) < 1e-6

    def test_proportions_preserved(self):
        col = pd.Series([1.0, 3.0])
        result = rescale(col)
        assert abs(result.iloc[0] - 25.0) < 1e-6
        assert abs(result.iloc[1] - 75.0) < 1e-6

    def test_zero_sum_guard_returns_unchanged(self):
        col = pd.Series([0.0, 0.0, 0.0])
        result = rescale(col)
        pd.testing.assert_series_equal(result, col)


class TestSumAndReplace:
    def test_wert_x_and_wert_y_are_summed(self):
        df = pd.DataFrame({"Wert_x": [10.0, 20.0], "Wert_y": [5.0, 15.0], "Name": ["A", "B"]})
        result = sum_and_replace(df, "Wert")
        assert "Wert" in result.columns
        assert "Wert_x" not in result.columns
        assert "Wert_y" not in result.columns
        assert list(result["Wert"]) == [15.0, 35.0]

    def test_nan_treated_as_zero(self):
        df = pd.DataFrame({"Wert_x": [10.0, None], "Wert_y": [None, 20.0]})
        result = sum_and_replace(df, "Wert")
        assert result["Wert"].iloc[0] == 10.0
        assert result["Wert"].iloc[1] == 20.0


class TestMergeAndDropCol:
    def test_col1_filled_by_col2_where_null(self):
        df = pd.DataFrame(
            {
                "col1": [None, "B", None],
                "col2": ["X", "Y", "Z"],
            }
        )
        result = merge_and_drop_col(df, "col1", "col2", "new_col")
        assert "new_col" in result.columns
        assert "col1" not in result.columns
        assert "col2" not in result.columns
        assert list(result["new_col"]) == ["X", "B", "Z"]

    def test_col1_value_takes_precedence_when_not_null(self):
        df = pd.DataFrame({"col1": ["A", "B"], "col2": ["X", "Y"]})
        result = merge_and_drop_col(df, "col1", "col2", "merged")
        assert list(result["merged"]) == ["A", "B"]


class TestSumDuplicatesBy:
    def test_duplicate_rows_are_summed(self):
        df = pd.DataFrame(
            {
                "Name": ["Apple", "Apple", "Google"],
                "Sektor": ["Tech", "Tech", "Tech"],
                "Wert": [100.0, 50.0, 200.0],
            }
        )
        result = sum_duplicates_by(df, "Wert", ["Name", "Sektor"])
        assert len(result) == 2
        apple_row = result[result["Name"] == "Apple"]
        assert apple_row["Wert"].iloc[0] == 150.0
        google_row = result[result["Name"] == "Google"]
        assert google_row["Wert"].iloc[0] == 200.0

    def test_unique_rows_unchanged_count(self):
        df = pd.DataFrame(
            {
                "Name": ["Apple", "Google", "Microsoft"],
                "Sektor": ["Tech", "Tech", "Tech"],
                "Wert": [100.0, 200.0, 300.0],
            }
        )
        result = sum_duplicates_by(df, "Wert", ["Name", "Sektor"])
        assert len(result) == 3


class TestPrepareSingleType:
    def test_filters_by_type(self):
        depot = pd.DataFrame(
            {
                "type": ["aktie", "krypto", "aktie"],
                "info": ["Apple Inc", "Bitcoin", "Microsoft Corp"],
                "ticker": ["AAPL", "BTC-USD", "MSFT"],
                "Wert": [100.0, 50.0, 200.0],
                "Standort": [None, None, None],
                "Sektor": [None, None, None],
            }
        )
        result = prepare_single_type(depot, "aktie")
        # krypto row is excluded; only the 2 aktie rows remain
        assert len(result) == 2
        # type column is dropped in the output (only info/ticker/Wert/Standort/Sektor kept)
        assert "type" not in result.columns

    def test_renames_info_to_name_and_ticker_to_emittententicker(self):
        depot = pd.DataFrame(
            {
                "type": ["aktie"],
                "info": ["Apple Inc"],
                "ticker": ["AAPL.DE"],
                "Wert": [100.0],
                "Standort": [None],
                "Sektor": [None],
            }
        )
        result = prepare_single_type(depot, "aktie")
        assert "Name" in result.columns
        assert "Emittententicker" in result.columns
        assert "info" not in result.columns
        assert "ticker" not in result.columns

    def test_ticker_split_removes_exchange_suffix(self):
        depot = pd.DataFrame(
            {
                "type": ["aktie"],
                "info": ["Apple Inc"],
                "ticker": ["AAPL.DE"],
                "Wert": [100.0],
                "Standort": [None],
                "Sektor": [None],
            }
        )
        result = prepare_single_type(depot, "aktie")
        assert result["Emittententicker"].iloc[0] == "AAPL"

    def test_name_uppercased(self):
        depot = pd.DataFrame(
            {
                "type": ["aktie"],
                "info": ["Apple Inc"],
                "ticker": ["AAPL"],
                "Wert": [100.0],
                "Standort": [None],
                "Sektor": [None],
            }
        )
        result = prepare_single_type(depot, "aktie")
        assert result["Name"].iloc[0] == "APPLE INC"

    def test_type_column_set_to_given_type(self):
        depot = pd.DataFrame(
            {
                "type": ["krypto"],
                "info": ["Bitcoin"],
                "ticker": ["BTC-USD"],
                "Wert": [500.0],
                "Standort": [None],
                "Sektor": [None],
            }
        )
        result = prepare_single_type(depot, "krypto")
        assert result["Type"].iloc[0] == "krypto"

    def test_returns_correct_columns(self):
        depot = pd.DataFrame(
            {
                "type": ["aktie"],
                "info": ["Apple Inc"],
                "ticker": ["AAPL"],
                "Wert": [100.0],
                "Standort": [None],
                "Sektor": [None],
            }
        )
        result = prepare_single_type(depot, "aktie")
        for col in ["Name", "Emittententicker", "Wert", "Standort", "Sektor", "Type"]:
            assert col in result.columns
