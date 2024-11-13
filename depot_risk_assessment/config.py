import pathlib
from dataclasses import dataclass

import pandas as pd

from depot_risk_assessment.transform_etfs import (
    download_zusammensetzung_as_csv,
    prepare_amundi_data,
    prepare_invesco_data,
    prepare_ishare_data,
    read_amundi_from,
    read_invesco_xlsx,
    read_ishare_from,
)


@dataclass
class ETFConfig:
    wkn: str
    editor: str
    url: str | None
    file_path: pathlib.Path
    zusammensetzung: pd.DataFrame
    total_value: float


@dataclass
class ETFHandler:
    etfs: list[ETFConfig]

    @classmethod
    def from_dict(
        cls, etf_dict: dict, depot: pd.DataFrame, sector_mapping: dict[str, str]
    ) -> "ETFHandler":
        etfs = []
        for key, value in etf_dict.items():
            total_value_etf = depot[depot["wkn"] == key]["Wert"].values[0]
            if value["editor"] == "iShares":
                url = value["url"]
                path = value["file_path"]
                download_zusammensetzung_as_csv(url, path)
                df = read_ishare_from(path)
                df = prepare_ishare_data(df, total_value_etf)
            elif value["editor"] == "amundi":
                path = value["file_path"]
                df = read_amundi_from(path)
                df = prepare_amundi_data(df, sector_mapping, total_value_etf)
            else:
                path = value["file_path"]
                df = read_invesco_xlsx(path)
                df = prepare_invesco_data(df, total_value_etf)
            etf = ETFConfig(
                key,
                value["editor"],
                value.get("url", None),
                path,
                df,
                total_value_etf,
            )
            etfs.append(etf)
        return ETFHandler(etfs)
