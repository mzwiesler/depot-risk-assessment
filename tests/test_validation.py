import pandas as pd
import pytest

from depot_risk_assessment.validation import (
    PipelineValidationError,
    validate_columns,
    validate_no_duplicate_key,
    validate_no_nulls,
    validate_value_balance,
)


class TestValidateValueBalance:
    def test_passes_within_tolerance(self):
        # Should not raise when delta is within default tolerance of 1.0
        validate_value_balance(100.5, 100.0, tolerance=1.0)

    def test_passes_at_exact_match(self):
        validate_value_balance(100.0, 100.0)

    def test_raises_outside_tolerance(self):
        with pytest.raises(PipelineValidationError):
            validate_value_balance(102.0, 100.0, tolerance=1.0)

    def test_raises_with_context_in_message(self):
        with pytest.raises(PipelineValidationError, match="my context"):
            validate_value_balance(200.0, 100.0, tolerance=1.0, context="my context")

    def test_passes_at_tolerance_boundary(self):
        # delta == tolerance should not raise (strictly greater than triggers error)
        validate_value_balance(101.0, 100.0, tolerance=1.0)


class TestValidateColumns:
    def test_passes_when_all_columns_present(self):
        df = pd.DataFrame({"Name": ["A"], "Wert": [1.0], "Sektor": ["Tech"]})
        validate_columns(df, ["Name", "Wert", "Sektor"])

    def test_raises_when_column_missing(self):
        df = pd.DataFrame({"Name": ["A"], "Wert": [1.0]})
        with pytest.raises(PipelineValidationError):
            validate_columns(df, ["Name", "Wert", "Sektor"])

    def test_raises_with_missing_column_name_in_message(self):
        df = pd.DataFrame({"Name": ["A"]})
        with pytest.raises(PipelineValidationError, match="Wert"):
            validate_columns(df, ["Name", "Wert"])

    def test_passes_with_extra_columns(self):
        df = pd.DataFrame({"Name": ["A"], "Wert": [1.0], "Extra": ["x"]})
        validate_columns(df, ["Name", "Wert"])


class TestValidateNoDuplicateKey:
    def test_passes_with_unique_keys(self):
        df = pd.DataFrame({"ISIN": ["US001", "US002", "US003"], "Wert": [1.0, 2.0, 3.0]})
        validate_no_duplicate_key(df, "ISIN")

    def test_raises_with_duplicate_keys(self):
        df = pd.DataFrame({"ISIN": ["US001", "US001", "US002"], "Wert": [1.0, 2.0, 3.0]})
        with pytest.raises(PipelineValidationError):
            validate_no_duplicate_key(df, "ISIN")

    def test_raises_with_duplicate_key_in_message(self):
        df = pd.DataFrame({"ISIN": ["DUP123", "DUP123"]})
        with pytest.raises(PipelineValidationError, match="DUP123"):
            validate_no_duplicate_key(df, "ISIN")

    def test_raises_with_context_in_message(self):
        df = pd.DataFrame({"ISIN": ["X", "X"]})
        with pytest.raises(PipelineValidationError, match="myctx"):
            validate_no_duplicate_key(df, "ISIN", context="myctx")


class TestValidateNoNulls:
    def test_passes_when_no_nulls(self):
        df = pd.DataFrame({"Name": ["Apple", "Google"], "Wert": [100.0, 200.0]})
        validate_no_nulls(df, ["Name", "Wert"])

    def test_raises_when_null_present(self):
        df = pd.DataFrame({"Name": ["Apple", None], "Wert": [100.0, 200.0]})
        with pytest.raises(PipelineValidationError):
            validate_no_nulls(df, ["Name"])

    def test_raises_with_column_name_in_message(self):
        df = pd.DataFrame({"Wert": [1.0, None]})
        with pytest.raises(PipelineValidationError, match="Wert"):
            validate_no_nulls(df, ["Wert"])

    def test_raises_with_context_in_message(self):
        df = pd.DataFrame({"Name": [None]})
        with pytest.raises(PipelineValidationError, match="nullctx"):
            validate_no_nulls(df, ["Name"], context="nullctx")

    def test_only_checks_specified_columns(self):
        df = pd.DataFrame({"Name": ["Apple", None], "Wert": [100.0, 200.0]})
        # Wert has no nulls, should pass even though Name has nulls
        validate_no_nulls(df, ["Wert"])
