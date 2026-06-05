import pathlib
from dataclasses import dataclass

import pandas as pd

from depot_risk_assessment.providers import get_provider


@dataclass
class TickerConfig:
    """Configuration for ETF ticker downloads."""

    wkn: str
    editor: str
    url: str
    file_name: str

    def file_path(self, folder: pathlib.Path) -> pathlib.Path:
        return folder / f"{self.file_name}.csv"


@dataclass
class ETFConfig:
    ticker_config: TickerConfig
    zusammensetzung: pd.DataFrame
    total_value: float


@dataclass
class ETFHandler:
    etfs: list[ETFConfig]

    @classmethod
    def from_dict(
        cls, etf_dict: dict, folder: str, depot: pd.DataFrame, sector_mapping: dict[str, str]
    ) -> "ETFHandler":
        etfs = []
        for key, value in etf_dict.items():
            total_value_etf = depot[depot["wkn"] == key]["Wert"].values[0]
            ticker_config = TickerConfig(
                wkn=key, editor=value["editor"], url=value.get("url", None), file_name=value["file_name"]
            )
            provider = get_provider(value["editor"])
            path = ticker_config.file_path(pathlib.Path(folder))
            df = provider.read(path)
            df = provider.normalize(df, total_value_etf, sector_mapping)
            etf = ETFConfig(
                ticker_config,
                df,
                total_value_etf,
            )
            etfs.append(etf)
        return ETFHandler(etfs)
