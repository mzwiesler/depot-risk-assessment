import logging
from functools import reduce

import pandas as pd

from depot_risk_assessment.mapping import (
    country_mapping_ishare,
    country_mapping_yahoo,
    sector_mapping_yahoo,
)

logger = logging.getLogger(__name__)


def rescale(col: pd.Series) -> pd.Series:
    if col.sum() == 0:
        return col
    return round(100 * col / col.sum(), 8)


def aggregate_gewichtung_by(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    return df.groupby(cols).agg({"Gewichtung": "sum"}).reset_index()


def sum_duplicates_by(df: pd.DataFrame, col_sum: str, cols_group: list[str]) -> pd.DataFrame:
    df_grouped = df.groupby(cols_group, dropna=False).agg({col_sum: "sum"})
    df = df.drop_duplicates(subset=cols_group)
    df = df.drop(columns=[col_sum])
    result = pd.merge(df, df_grouped, on=cols_group)
    return result


def merge_same_editors(dfs: list[pd.DataFrame], merge_cols: list[str], col_to_keep: list[str]) -> pd.DataFrame:
    dfs = [df[col_to_keep] for df in dfs]
    df = reduce(lambda x, y: x.merge(y, on=merge_cols, how="outer"), dfs)
    return df


def sum_and_replace(df: pd.DataFrame, col_contains: str) -> pd.DataFrame:
    col_names_containing = [col for col in df.columns if col_contains in col]
    df[col_names_containing] = df[col_names_containing].fillna(0)
    row_sum = df[col_names_containing].sum(axis=1)
    df = df.drop(columns=col_names_containing)
    df[col_contains] = row_sum
    return df


def merge_and_drop_col(df: pd.DataFrame, col1: str, col2: str, new_col: str) -> pd.DataFrame:
    df[new_col] = df[col1].fillna(df[col2])
    df = df.drop(columns=[col1, col2])
    return df


def prepare_data_by_isin(df: pd.DataFrame, ex_isin_info: pd.DataFrame, merge_cols: list[str]) -> pd.DataFrame:
    """Enrich ISIN-keyed holdings with Yahoo Finance sector/country data and deduplicate."""
    merged_isin = df.copy()
    if "Wert_x" in merged_isin.columns or "Wert_y" in merged_isin.columns:
        merged_isin = sum_and_replace(merged_isin, "Wert")
    if "Name_x" in merged_isin.columns and "Name_y" in merged_isin.columns:
        merged_isin = merge_and_drop_col(merged_isin, "Name_x", "Name_y", "Name")
    merged_isin = merged_isin.merge(ex_isin_info, on="ISIN", how="left")
    merged_isin["Standort_y"] = merged_isin["Standort_y"].map(country_mapping_yahoo)
    merged_isin = merge_and_drop_col(merged_isin, "Standort_x", "Standort_y", "Standort")
    merged_isin["Sektor_y"] = merged_isin["Sektor_y"].map(sector_mapping_yahoo)
    merged_isin = merge_and_drop_col(merged_isin, "Sektor_x", "Sektor_y", "Sektor")
    merged_isin = merge_and_drop_col(merged_isin, "Name_x", "Name_y", "Name")

    merged_isin = sum_duplicates_by(merged_isin, "Wert", merge_cols)
    merged_isin = merged_isin.drop(columns=["ISIN", "Symbol"])
    merged_isin["Standort"] = merged_isin["Standort"].replace(country_mapping_ishare)
    merged_isin["Emittententicker"] = merged_isin["Emittententicker"].fillna(merged_isin["Name"])
    return merged_isin


def prepare_data_by_ticker(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse ticker-keyed iShares holdings by summing values and resolving duplicate name columns."""
    df["Name"] = df["Name_x"].fillna(df["Name_y"])
    df = sum_and_replace(df, "Wert")
    df = merge_and_drop_col(df, "Name_x", "Name_y", "Name")
    df = merge_and_drop_col(df, "Sektor_x", "Sektor_y", "Sektor")
    df_grouped = df.groupby("Name").agg({"Wert": "sum"})
    df = df.drop_duplicates(subset=["Name"]).drop(columns=["Wert"])
    df = df.merge(df_grouped, on="Name", how="left")
    multiple_ticker = df.groupby("Emittententicker").agg({"Name": "count", "Sektor": "count", "Standort": "count"})
    multiple_ticker = multiple_ticker[multiple_ticker["Name"] > 1]
    df["Type"] = "ETF"
    return df


def prepare_single_type(depot: pd.DataFrame, type: str) -> pd.DataFrame:
    type_depot = depot[depot["type"] == type][["info", "ticker", "Wert", "Standort", "Sektor"]].copy()
    type_depot = type_depot.rename(columns={"info": "Name", "ticker": "Emittententicker"})
    type_depot["Name"] = type_depot["Name"].str.upper()
    type_depot["Emittententicker"] = type_depot["Emittententicker"].str.split(".").str[0]
    type_depot["Standort"] = type_depot["Standort"].map(country_mapping_yahoo)
    type_depot["Standort"] = type_depot["Standort"].replace(country_mapping_ishare)
    type_depot["Sektor"] = type_depot["Sektor"].map(sector_mapping_yahoo)
    type_depot["Type"] = type
    return type_depot
