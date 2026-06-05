import logging

import pandas as pd

from depot_risk_assessment.dataframe_ops import (
    merge_same_editors,
    prepare_data_by_isin,
    prepare_data_by_ticker,
    sum_and_replace,
)
from depot_risk_assessment.etf_handling import ETFHandler
from depot_risk_assessment.isin_cache import get_infos_from_yahoo
from depot_risk_assessment.validation import (
    validate_editor,
    validate_ishare,
    validate_no_duplicate_key,
    validate_value_balance,
)

logger = logging.getLogger(__name__)


def process_isin_group(
    etf_handler: ETFHandler,
    ex_isin_info: pd.DataFrame,
    editor: str,
    merge_cols: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    etfs = [etf.zusammensetzung for etf in etf_handler.etfs if etf.ticker_config.editor == editor]
    if not etfs:
        return pd.DataFrame(), ex_isin_info

    logger.info(f"Processing {editor} ETFs ({len(etfs)} funds)")

    if len(etfs) > 1:
        merged = merge_same_editors(
            etfs,
            ["ISIN", "Name", "Sektor", "Standort"],
            ["ISIN", "Name", "Sektor", "Standort", "Wert"],
        )
        merged = sum_and_replace(merged, "Wert")
    else:
        merged = etfs[0]

    total_value = merged["Wert"].sum()
    logger.info(f"{editor} total value: €{total_value:,.2f}")
    validate_editor(etf_handler, total_value, editor)

    add_info = get_infos_from_yahoo(merged, ex_isin_info)
    if len(add_info) > 0:
        logger.info(f"Found {len(add_info)} new {editor} entries from Yahoo")
        ex_isin_info = pd.concat([ex_isin_info, add_info])

    merged["ISIN"] = merged["ISIN"].fillna(merged["Name"])
    validate_no_duplicate_key(merged, "ISIN", context=editor)

    return merged, ex_isin_info


def merge_isin_group(
    etf_handler: ETFHandler,
    ex_isin_info: pd.DataFrame,
    isin_editors: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    merge_cols = ["Emittententicker", "Standort"]
    dfs: list[pd.DataFrame] = []

    for editor in isin_editors:
        df, ex_isin_info = process_isin_group(etf_handler, ex_isin_info, editor, merge_cols)
        if not df.empty:
            dfs.append(df)

    if not dfs:
        return pd.DataFrame(), ex_isin_info

    ex_isin_info["Emittententicker"] = ex_isin_info["Symbol"].str.split(".").str[0]

    if len(dfs) == 1:
        merged_isin = dfs[0]
    else:
        keep_cols = ["ISIN", "Name", "Wert", "Sektor", "Standort"]
        combined = pd.concat([df[[c for c in keep_cols if c in df.columns]] for df in dfs], ignore_index=True)
        wert_sum = combined.groupby("ISIN", dropna=False)["Wert"].sum().reset_index()
        first_vals = combined.groupby("ISIN", dropna=False).first().reset_index()
        merged_isin = wert_sum.merge(first_vals.drop(columns=["Wert"]), on="ISIN", how="left")

    merged_isin = prepare_data_by_isin(merged_isin, ex_isin_info, merge_cols)

    expected_value = sum(etf.total_value for etf in etf_handler.etfs if etf.ticker_config.editor in isin_editors)
    validate_value_balance(merged_isin["Wert"].sum(), expected_value, tolerance=1.0, context="ISIN group merge")

    return merged_isin, ex_isin_info


def merge_ticker_group(etf_handler: ETFHandler) -> pd.DataFrame:
    ticker_etfs = [etf.zusammensetzung for etf in etf_handler.etfs if etf.ticker_config.editor == "iShares"]
    if not ticker_etfs:
        return pd.DataFrame()

    logger.info(f"Processing iShares ETFs ({len(ticker_etfs)} funds)")

    ishares_merged = merge_same_editors(
        ticker_etfs,
        ["Emittententicker", "Name", "Sektor", "Standort"],
        ["Emittententicker", "Name", "Sektor", "Standort", "Wert"],
    )
    ishares_merged = sum_and_replace(ishares_merged, "Wert")
    ishares_merged["Emittententicker"] = ishares_merged["Emittententicker"].fillna(ishares_merged["Name"])
    ishares_merged["Emittententicker"] = ishares_merged["Emittententicker"].str.replace(" ", "-")

    total_value = ishares_merged["Wert"].sum()
    logger.info(f"iShares total value: €{total_value:,.2f}")
    validate_ishare(ishares_merged, etf_handler)

    return ishares_merged


def merge_all_etfs(
    etf_handler: ETFHandler,
    ex_isin_info: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    isin_editors = list({etf.ticker_config.editor for etf in etf_handler.etfs if etf.ticker_config.editor != "iShares"})
    isin_editors.sort()

    isin_merged, ex_isin_info = merge_isin_group(etf_handler, ex_isin_info, isin_editors)
    ticker_merged = merge_ticker_group(etf_handler)

    merge_cols = ["Emittententicker", "Standort"]
    if isin_merged.empty:
        merged_df = ticker_merged
    elif ticker_merged.empty:
        merged_df = isin_merged
    else:
        merged_df = ticker_merged.merge(isin_merged, on=merge_cols, how="outer")
        merged_df = prepare_data_by_ticker(merged_df)

    expected_total = sum(etf.total_value for etf in etf_handler.etfs)
    validate_value_balance(merged_df["Wert"].sum(), expected_total, tolerance=1.0, context="total ETF merge")
    logger.info(f"Total merged ETF value: €{merged_df['Wert'].sum():,.2f}")

    return merged_df, ex_isin_info
