import pathlib

import pandas as pd

from depot_risk_assessment.providers._base import _finalize, register


class XtrackersProvider:
    editor_name = "xtrackers"
    identifier_type = "isin"

    def read(self, file_path: pathlib.Path) -> pd.DataFrame:
        return pd.read_csv(file_path, header="infer", skiprows=3, sep=",")

    def normalize(self, df: pd.DataFrame, total_value: float, sector_mapping: dict[str, str]) -> pd.DataFrame:
        df = df[~df["Weighting"].isna()]
        df = df.rename(columns={"Weighting": "Gewichtung"})
        df["ISIN"] = df["ISIN"].fillna(df["Name"])
        df = _finalize(df, total_value)
        return df


register(XtrackersProvider())
