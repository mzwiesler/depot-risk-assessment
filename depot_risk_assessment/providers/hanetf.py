import pathlib

import pandas as pd

from depot_risk_assessment.providers._base import _finalize, register


class HanetfProvider:
    editor_name = "hanetf"
    identifier_type = "isin"

    def read(self, file_path: pathlib.Path) -> pd.DataFrame:
        return pd.read_csv(file_path, header="infer", skiprows=3)

    def normalize(self, df: pd.DataFrame, total_value: float, sector_mapping: dict[str, str]) -> pd.DataFrame:
        df = df[~df["Weight"].isna()]
        df = df.rename(columns={"Security Description": "Name", "Weight": "Gewichtung"})
        df["Name"] = df["Name"].str.split("USD").str[0].str.strip()
        df["ISIN"] = df["ISIN"].fillna(df["Name"])
        df = _finalize(df, total_value)
        return df


register(HanetfProvider())
