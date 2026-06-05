import pathlib

import pandas as pd

from depot_risk_assessment.dataframe_ops import rescale
from depot_risk_assessment.providers._base import register


class InvescoProvider:
    editor_name = "invesco"
    identifier_type = "isin"

    def read(self, file_path: pathlib.Path) -> pd.DataFrame:
        return pd.read_csv(file_path, header=1, skiprows=4)

    def normalize(self, df: pd.DataFrame, total_value: float, sector_mapping: dict[str, str]) -> pd.DataFrame:
        df = df[~df["Weight"].isna()]
        df = df.rename(columns={"Full name": "Name", "Weight": "Gewichtung"})
        df["Name"] = df["Name"].str.split("USD").str[0].str.strip()
        df["ISIN"] = df["ISIN"].fillna(df["Name"])
        df["Gewichtung"] = rescale(df["Gewichtung"])
        df["Wert"] = round(df["Gewichtung"] * total_value / 100, 2)
        return df


register(InvescoProvider())
