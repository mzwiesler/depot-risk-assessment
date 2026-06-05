import pathlib

import pandas as pd

from depot_risk_assessment.dataframe_ops import rescale
from depot_risk_assessment.providers._base import register


class AmundiProvider:
    editor_name = "amundi"
    identifier_type = "isin"

    def read(self, file_path: pathlib.Path) -> pd.DataFrame:
        df = pd.read_csv(file_path, header="infer", skiprows=19, sep=",")
        df = df[~df["Gewichtung"].isna()]
        return df

    def normalize(self, df: pd.DataFrame, total_value: float, sector_mapping: dict[str, str]) -> pd.DataFrame:
        df = df.drop(columns="Unnamed: 0")
        df["Name"] = df["Name"].fillna(df["Anlageklasse"])
        df = df[df["Gewichtung"] != 0]
        df = df.rename(columns={"Land": "Standort"})
        df["Sektor"] = df["Sektor"].map(sector_mapping)
        df["Gewichtung"] = rescale(df["Gewichtung"])
        df["Wert"] = round(df["Gewichtung"] * total_value / 100, 2)
        return df


register(AmundiProvider())
