import logging
import pathlib

import pandas as pd

from depot_risk_assessment.ticker_config import ticker_config
from depot_risk_assessment.etf_handling import ETFHandler
from depot_risk_assessment.finance_data import get_infos_for, get_infos_from_yahoo
from depot_risk_assessment.mapping import sector_mapping
from depot_risk_assessment.etf_transformations import (
    merge_same_editors,
    prepare_data_by_isin,
    prepare_data_by_ticker,
    prepare_single_type,
    sum_and_replace,
)
from depot_risk_assessment.validation import (
    validate_editor,
    validate_etf,
    validate_ishare,
)

logger = logging.getLogger(__name__)


def load_and_enrich_depot(path_to_depot: str, eval_date: str) -> pd.DataFrame:
    """Load depot data and enrich it with market information and calculations."""
    logger.info(f"Loading depot from {path_to_depot}")
    depot = pd.read_csv(pathlib.Path(path_to_depot), header="infer", sep=";")
    logger.info(f"Loaded {len(depot)} positions from depot")

    logger.info("Fetching market info for tickers")
    infos = get_infos_for(depot["ticker"].to_list())
    depot = pd.concat([depot, infos], axis=1)

    logger.info(f"Calculating values for date: {eval_date}")
    depot["Wert"] = depot["Price"] * depot[eval_date]
    depot["Percentage"] = depot["Wert"] / depot["Wert"].sum() * 100

    total_value = depot["Wert"].sum()
    logger.info(f"Total depot value: €{total_value:,.2f}")

    return depot


def process_amundi_etfs(etf_handler: ETFHandler, ex_isin_info: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Process Amundi ETF data and retrieve missing information."""
    logger.info("Processing Amundi ETFs")

    amundi_etfs = [etf.zusammensetzung for etf in etf_handler.etfs if etf.ticker_config.editor == "amundi"]
    logger.debug(f"Found {len(amundi_etfs)} Amundi ETFs")

    amundi_merged = merge_same_editors(
        amundi_etfs,
        ["ISIN", "Name", "Sektor", "Standort"],
        ["ISIN", "Name", "Sektor", "Standort", "Wert"],
    )
    amundi_merged = sum_and_replace(amundi_merged, "Wert")

    total_value = amundi_merged["Wert"].sum()
    logger.info(f"Amundi total value: €{total_value:,.2f}")
    validate_editor(etf_handler, total_value, "amundi")

    logger.info("Fetching additional Amundi info from Yahoo")
    add_amundi_info = get_infos_from_yahoo(amundi_merged, ex_isin_info)

    if len(add_amundi_info) > 0:
        logger.info(f"Found {len(add_amundi_info)} new Amundi entries")
        ex_isin_info = pd.concat([ex_isin_info, add_amundi_info])
    else:
        logger.info("No new Amundi data found")

    amundi_merged["ISIN"] = amundi_merged["ISIN"].fillna(amundi_merged["Name"])
    assert len(amundi_merged) == len(amundi_merged["ISIN"].unique()), "Duplicate ISINs found in Amundi data"
    logger.debug(f"Amundi data has {len(amundi_merged)} unique entries")

    return amundi_merged, ex_isin_info


def process_invesco_etfs(etf_handler: ETFHandler, ex_isin_info: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Process Invesco ETF data and retrieve missing information."""
    logger.info("Processing Invesco ETFs")

    invesco_df = [etf.zusammensetzung for etf in etf_handler.etfs if etf.ticker_config.editor == "invesco"][0]
    logger.debug(f"Invesco data has {len(invesco_df)} entries")

    logger.info("Fetching additional Invesco info from Yahoo")
    add_invesco_info = get_infos_from_yahoo(invesco_df, ex_isin_info)

    if len(add_invesco_info) > 0:
        logger.info(f"Found {len(add_invesco_info)} new Invesco entries")
        ex_isin_info = pd.concat([ex_isin_info, add_invesco_info])
    else:
        logger.info("No new Invesco data found")

    invesco_df["ISIN"] = invesco_df["ISIN"].fillna(invesco_df["Name"])
    ex_isin_info["Emittententicker"] = ex_isin_info["Symbol"].str.split(".").str[0]

    total_value = invesco_df["Wert"].sum()
    logger.info(f"Invesco total value: €{total_value:,.2f}")
    validate_editor(etf_handler, total_value, "invesco")

    assert len(invesco_df) == len(invesco_df["ISIN"].unique()), "Duplicate ISINs found in Invesco data"
    logger.debug(f"Invesco data has {len(invesco_df)} unique entries")

    return invesco_df, ex_isin_info


def process_ishares_etfs(etf_handler: ETFHandler) -> pd.DataFrame:
    """Process iShares ETF data."""
    logger.info("Processing iShares ETFs")

    ishares_etfs = [etf.zusammensetzung for etf in etf_handler.etfs if etf.ticker_config.editor == "iShares"]
    logger.debug(f"Found {len(ishares_etfs)} iShares ETFs")

    ishares_merged = merge_same_editors(
        ishares_etfs,
        ["Emittententicker", "Name", "Sektor", "Standort"],
        ["Emittententicker", "Name", "Sektor", "Standort", "Wert"],
    )
    ishares_merged = sum_and_replace(ishares_merged, "Wert")
    ishares_merged["Emittententicker"] = ishares_merged["Emittententicker"].fillna(ishares_merged["Name"])
    ishares_merged["Emittententicker"] = ishares_merged["Emittententicker"].str.replace(" ", "-")

    total_value = ishares_merged["Wert"].sum()
    logger.info(f"iShares total value: €{total_value:,.2f}")
    validate_ishare(ishares_merged, etf_handler)

    logger.debug(f"iShares data has {len(ishares_merged)} unique entries")

    return ishares_merged


def merge_etf_data(
    amundi_merged: pd.DataFrame,
    invesco_df: pd.DataFrame,
    ishares_merged: pd.DataFrame,
    ex_isin_info: pd.DataFrame,
    etf_handler: ETFHandler,
) -> pd.DataFrame:
    """Merge all ETF data from different providers."""
    logger.info("Merging ETF data from all providers")

    # Merge Amundi and Invesco
    logger.debug("Merging Amundi and Invesco data")
    merged_isin = amundi_merged.merge(invesco_df[["Name", "ISIN", "Wert"]], on="ISIN", how="outer")
    merge_cols = ["Emittententicker", "Standort"]
    merged_isin = prepare_data_by_isin(merged_isin, ex_isin_info, merge_cols)

    expected_value = invesco_df["Wert"].sum() + amundi_merged["Wert"].sum()
    actual_value = merged_isin["Wert"].sum()
    assert abs(expected_value - actual_value) < 0.1, f"Value mismatch: expected {expected_value}, got {actual_value}"
    logger.debug(f"Amundi+Invesco merged value: €{actual_value:,.2f}")

    # Merge with iShares
    logger.debug("Merging with iShares data")
    merged_df = ishares_merged.merge(merged_isin, on=merge_cols, how="outer")
    merged_df = prepare_data_by_ticker(merged_df)

    expected_total = sum([etf.total_value for etf in etf_handler.etfs])
    actual_total = merged_df["Wert"].sum()
    assert abs(actual_total - expected_total) < 1, (
        f"Total ETF value mismatch: expected {expected_total}, got {actual_total}"
    )
    logger.info(f"Total merged ETF value: €{actual_total:,.2f}")

    return merged_df


def save_isin_info(ex_isin_info: pd.DataFrame, path: str = "./data/isin_information.csv") -> None:
    """Save updated ISIN information to CSV."""
    logger.info(f"Saving ISIN information to {path}")
    ex_isin_info.to_csv(path, index=False, sep=",", encoding="utf-8", mode="w")
    logger.debug(f"Saved {len(ex_isin_info)} ISIN records")


def main(
    eval_date: str,
    path_to_depot: str,
    path_to_isin_info: str,
    source_path: str,
    sink_path: str,
    ticker_config: dict,
):
    """
    Main function to process depot risk assessment.

    Args:
        eval_date: Date for evaluation (format: DD.MM.YYYY)
        path_to_depot: Path to depot CSV file
        path_to_isin_info: Path to ISIN information CSV
        source_path: Path to source data directory
        sink_path: Path to output merged depot CSV
        ticker_config: Configuration for tickers
    """
    logger.info("=" * 80)
    logger.info("Starting depot risk assessment")
    logger.info(f"Evaluation date: {eval_date}")
    logger.info("=" * 80)

    # Load and enrich depot
    depot = load_and_enrich_depot(path_to_depot, eval_date)

    # Initialize ETF handler
    logger.info("Initializing ETF handler")
    etf_handler = ETFHandler.from_dict(ticker_config, source_path, depot, sector_mapping)
    etf_total_value = depot[depot["type"] == "etf"]["Wert"].sum()
    logger.info(f"Total ETF value in depot: €{etf_total_value:,.2f}")
    validate_etf(etf_handler, etf_total_value)

    # Load existing ISIN information
    logger.info(f"Loading ISIN information from {path_to_isin_info}")
    ex_isin_info = pd.read_csv(pathlib.Path(path_to_isin_info), header="infer", sep=",")
    logger.debug(f"Loaded {len(ex_isin_info)} existing ISIN records")

    # Process each ETF provider
    amundi_merged, ex_isin_info = process_amundi_etfs(etf_handler, ex_isin_info)
    invesco_df, ex_isin_info = process_invesco_etfs(etf_handler, ex_isin_info)

    # Save updated ISIN information
    save_isin_info(ex_isin_info)

    # Process iShares
    ishares_merged = process_ishares_etfs(etf_handler)

    # Merge all ETF data
    merged_df = merge_etf_data(amundi_merged, invesco_df, ishares_merged, ex_isin_info, etf_handler)

    # Prepare other asset types
    logger.info("Processing other asset types")
    aktien_depot = prepare_single_type(depot, "aktie")
    logger.info(f"Stocks value: €{aktien_depot['Wert'].sum():,.2f}")

    krypto_depot = prepare_single_type(depot, "krypto")
    logger.info(f"Crypto value: €{krypto_depot['Wert'].sum():,.2f}")

    # Merge all data
    logger.info("Creating final merged depot")
    depot_merged = pd.concat([merged_df, aktien_depot, krypto_depot], axis=0)

    expected_total = depot[depot["type"].isin(["etf", "aktie", "krypto"])]["Wert"].sum()
    actual_total = depot_merged["Wert"].sum()
    assert abs(actual_total - expected_total) < 1, (
        f"Final value mismatch: expected {expected_total}, got {actual_total}"
    )
    logger.info(f"Final merged depot value: €{actual_total:,.2f}")

    depot_merged["Name"] = depot_merged.groupby(by=["Emittententicker", "Sektor", "Standort"], dropna=False)[
        "Name"
    ].transform("first")

    # Save final output
    logger.info(f"Saving merged depot to {sink_path}")
    depot_merged.to_csv(sink_path, index=False, sep=",", encoding="utf-8", mode="w", na_rep="NA")
    logger.info(f"Saved {len(depot_merged)} merged positions")

    logger.info("=" * 80)
    logger.info("Depot risk assessment completed successfully")
    logger.info("=" * 80)


if __name__ == "__main__":
    eval_date = "19.03.2025"

    main(
        eval_date=eval_date,
        path_to_depot="./data/depot.csv",
        path_to_isin_info="./data/isin_information.csv",
        source_path="./downloads",
        sink_path="./data/depot_merged.csv",
        ticker_config=ticker_config,
    )
