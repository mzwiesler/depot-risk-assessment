import pathlib

import pandas as pd

from depot_risk_assessment.providers._base import _finalize, register


class ISharesProvider:
    editor_name = "iShares"
    identifier_type = "ticker"

    def read(self, file_path: pathlib.Path) -> pd.DataFrame:
        return pd.read_csv(file_path, header="infer", sep=",", skiprows=2)

    def normalize(self, df: pd.DataFrame, total_value: float, sector_mapping: dict[str, str]) -> pd.DataFrame:
        df = df.rename(columns={"Gewichtung (%)": "Gewichtung", "Marktwährung": "Währung"})
        df = df[df["Name"].notnull()]
        if "Sector" in df.columns:
            df = df.rename(columns={"Sector": "Sektor"})
        df["Sektor"] = df["Sektor"].str.strip()
        df["Gewichtung"] = df["Gewichtung"].str.replace("%", "").str.replace(",", ".").astype(float)
        df = df[df["Gewichtung"] != 0]
        df = _finalize(df, total_value)
        return df


register(ISharesProvider())
