import logging
import pathlib

import pandas as pd

from depot_risk_assessment.dataframe_ops import prepare_single_type
from depot_risk_assessment.etf_handling import ETFHandler
from depot_risk_assessment.finance_data import get_infos_for
from depot_risk_assessment.mapping import sector_mapping
from depot_risk_assessment.merge import merge_all_etfs
from depot_risk_assessment.ticker_config import ticker_config
from depot_risk_assessment.validation import validate_etf, validate_value_balance

logger = logging.getLogger(__name__)


def load_and_enrich_depot(path_to_depot: str, eval_date: str) -> pd.DataFrame:
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


def save_isin_info(ex_isin_info: pd.DataFrame, path: str = "./data/isin_information.csv") -> None:
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
) -> None:
    logger.info("=" * 80)
    logger.info("Starting depot risk assessment")
    logger.info(f"Evaluation date: {eval_date}")
    logger.info("=" * 80)

    depot = load_and_enrich_depot(path_to_depot, eval_date)

    logger.info("Initializing ETF handler")
    etf_handler = ETFHandler.from_dict(ticker_config, source_path, depot, sector_mapping)
    etf_total_value = depot[depot["type"] == "etf"]["Wert"].sum()
    logger.info(f"Total ETF value in depot: €{etf_total_value:,.2f}")
    validate_etf(etf_handler, etf_total_value)

    logger.info(f"Loading ISIN information from {path_to_isin_info}")
    ex_isin_info = pd.read_csv(pathlib.Path(path_to_isin_info), header="infer", sep=",")
    logger.debug(f"Loaded {len(ex_isin_info)} existing ISIN records")

    merged_df, ex_isin_info = merge_all_etfs(etf_handler, ex_isin_info)

    save_isin_info(ex_isin_info)

    logger.info("Processing other asset types")
    aktien_depot = prepare_single_type(depot, "aktie")
    logger.info(f"Stocks value: €{aktien_depot['Wert'].sum():,.2f}")

    krypto_depot = prepare_single_type(depot, "krypto")
    logger.info(f"Crypto value: €{krypto_depot['Wert'].sum():,.2f}")

    logger.info("Creating final merged depot")
    depot_merged = pd.concat([merged_df, aktien_depot, krypto_depot], axis=0)

    expected_total = depot[depot["type"].isin(["etf", "aktie", "krypto"])]["Wert"].sum()
    actual_total = depot_merged["Wert"].sum()
    validate_value_balance(actual_total, expected_total, tolerance=1.0, context="final depot merge")
    logger.info(f"Final merged depot value: €{actual_total:,.2f}")

    depot_merged["Name"] = depot_merged.groupby(by=["Emittententicker", "Sektor", "Standort"], dropna=False)[
        "Name"
    ].transform("first")

    logger.info(f"Saving merged depot to {sink_path}")
    depot_merged.to_csv(sink_path, index=False, sep=",", encoding="utf-8", mode="w", na_rep="NA")
    logger.info(f"Saved {len(depot_merged)} merged positions")

    logger.info("=" * 80)
    logger.info("Depot risk assessment completed successfully")
    logger.info("=" * 80)


if __name__ == "__main__":
    eval_date = "02.02.2026"

    main(
        eval_date=eval_date,
        path_to_depot="./data/depot.csv",
        path_to_isin_info="./data/isin_information.csv",
        source_path="./downloads",
        sink_path="./data/depot_merged.csv",
        ticker_config=ticker_config,
    )
