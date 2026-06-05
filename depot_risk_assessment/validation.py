import pandas as pd

from depot_risk_assessment.etf_handling import ETFHandler


class PipelineValidationError(Exception):
    pass


def validate_value_balance(actual: float, expected: float, tolerance: float = 1.0, context: str = "") -> None:
    if abs(actual - expected) > tolerance:
        raise PipelineValidationError(
            f"Value mismatch{' in ' + context if context else ''}: "
            f"expected {expected:.2f}, got {actual:.2f} (delta={abs(actual - expected):.2f})"
        )


def validate_columns(df: pd.DataFrame, required: list[str], context: str = "") -> None:
    missing = set(required) - set(df.columns)
    if missing:
        raise PipelineValidationError(f"Missing required columns{' in ' + context if context else ''}: {missing}")


def validate_no_duplicate_key(df: pd.DataFrame, key_col: str, context: str = "") -> None:
    dupes = df[df[key_col].duplicated()]
    if len(dupes) > 0:
        raise PipelineValidationError(
            f"Duplicate keys in '{key_col}'{' (' + context + ')' if context else ''}: {dupes[key_col].tolist()[:5]}"
        )


def validate_no_nulls(df: pd.DataFrame, columns: list[str], context: str = "") -> None:
    for col in columns:
        null_count = df[col].isna().sum()
        if null_count > 0:
            raise PipelineValidationError(
                f"Found {null_count} null values in '{col}'{' (' + context + ')' if context else ''}"
            )


def validate_etf(etf_handler: ETFHandler, depot_wert: float) -> None:
    total = sum(etf.total_value for etf in etf_handler.etfs)
    validate_value_balance(total, depot_wert, tolerance=0.01, context="ETF total vs depot")


def validate_editor(etf_handler: ETFHandler, wert: float, editor: str) -> None:
    total = sum(etf.total_value for etf in etf_handler.etfs if etf.ticker_config.editor == editor)
    validate_value_balance(wert, total, tolerance=10, context=f"editor '{editor}'")


def validate_ishare(df: pd.DataFrame, etf_handler: ETFHandler) -> None:
    validate_editor(etf_handler, df["Wert"].sum(), "iShares")
    validate_no_nulls(df, ["Emittententicker", "Standort", "Wert"], context="iShares")
    grouped = df.groupby(["Emittententicker", "Standort"]).agg({"Standort": "count"})
    if (grouped > 1)["Standort"].sum() > 0:
        raise PipelineValidationError("Duplicate (Emittententicker, Standort) pairs in iShares data")
