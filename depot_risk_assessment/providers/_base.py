import pathlib
from typing import Protocol

import pandas as pd

from depot_risk_assessment.dataframe_ops import rescale


class ETFProvider(Protocol):
    editor_name: str
    identifier_type: str  # "isin" or "ticker"

    def read(self, file_path: pathlib.Path) -> pd.DataFrame: ...

    def normalize(self, df: pd.DataFrame, total_value: float, sector_mapping: dict[str, str]) -> pd.DataFrame: ...


_REGISTRY: dict[str, ETFProvider] = {}


def register(provider: ETFProvider) -> None:
    _REGISTRY[provider.editor_name] = provider


def get_provider(editor_name: str) -> ETFProvider:
    if editor_name not in _REGISTRY:
        raise ValueError(f"Unknown provider: {editor_name}. Registered: {list(_REGISTRY.keys())}")
    return _REGISTRY[editor_name]


def _finalize(df: pd.DataFrame, total_value: float) -> pd.DataFrame:
    df["Gewichtung"] = rescale(df["Gewichtung"])
    df["Wert"] = round(df["Gewichtung"] * total_value / 100, 2)
    return df
