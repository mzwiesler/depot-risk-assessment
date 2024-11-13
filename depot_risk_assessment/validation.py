import pandas as pd

from depot_risk_assessment.config import ETFHandler


def validate_etf(etf_handler: ETFHandler, depot_wert: float) -> None:
    assert abs(sum([etf.total_value for etf in etf_handler.etfs]) - depot_wert) < 0.01


def validate_editor(etf_handler: ETFHandler, wert: float, editor: str) -> None:
    assert (
        abs(
            wert
            - sum([etf.total_value for etf in etf_handler.etfs if etf.editor == editor])
        )
        < 0.2
    )


def validate_ishare(df: pd.DataFrame, etf_handler: ETFHandler) -> None:
    validate_editor(etf_handler, df["Wert"].sum(), "iShares")
    assert df["Emittententicker"].isna().sum() == 0
    assert df["Standort"].isna().sum() == 0
    assert df["Wert"].isna().sum() == 0
    assert (
        df.groupby(["Emittententicker", "Standort"]).agg({"Standort": "count"}) > 1
    )["Standort"].sum() == 0
