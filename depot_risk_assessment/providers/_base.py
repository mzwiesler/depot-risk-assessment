import pathlib
from typing import Protocol

import pandas as pd


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
