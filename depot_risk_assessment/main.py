import logging
import pathlib
from dataclasses import dataclass

import pandas as pd

from depot_risk_assessment.config import ETFHandler
from depot_risk_assessment.finance_data import get_infos_for, get_infos_from_yahoo
from depot_risk_assessment.mapping import sector_mapping
from depot_risk_assessment.transform_etfs import (
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


def main(
    eval_date: str,
    path_to_depot: str,
    path_to_isin_info: str,
    sink_path: str,
    ticker_config: dict,
):
    depot = pd.read_csv(pathlib.Path(path_to_depot), header="infer", sep=";")
    infos = get_infos_for(depot["ticker"].to_list())
    depot = pd.concat([depot, infos], axis=1)
    depot["Wert"] = depot["Price"] * depot[eval_date]
    depot["Percentage"] = depot["Wert"] / depot["Wert"].sum() * 100
    logging.info(f"Total value: {depot['Wert'].sum()}")
    # depot.groupby("type").agg({"Wert": "sum", "Percentage": "sum"})
    # After checking for new data, we can adjust the ISIN column
    etf_handler = ETFHandler.from_dict(ticker_config, depot, sector_mapping)
    depot[depot["type"] == "etf"]["Wert"].sum()
    validate_etf(etf_handler, depot[depot["type"] == "etf"]["Wert"].sum())
    ex_isin_info = pd.read_csv(pathlib.Path(path_to_isin_info), header="infer", sep=",")

    # Prepare Amundi data
    amundi_merged = merge_same_editors(
        [etf.zusammensetzung for etf in etf_handler.etfs if etf.editor == "amundi"],
        ["ISIN", "Name", "Sektor", "Standort"],
        ["ISIN", "Name", "Sektor", "Standort", "Wert"],
    )
    amundi_merged = sum_and_replace(amundi_merged, "Wert")
    validate_editor(etf_handler, amundi_merged["Wert"].sum(), "amundi")
    add_amundi_info = get_infos_from_yahoo(amundi_merged, ex_isin_info)

    if len(add_amundi_info) > 0:
        logger.info("New data found")
        ex_isin_info = pd.concat([ex_isin_info, add_amundi_info])

    # After checking for new data, we can adjust the ISIN column
    amundi_merged["ISIN"] = amundi_merged["ISIN"].fillna(amundi_merged["Name"])
    assert len(amundi_merged) == len(amundi_merged["ISIN"].unique())

    # Prepare Invesco data
    invesco_df = [
        etf.zusammensetzung for etf in etf_handler.etfs if etf.editor == "invesco"
    ][0]
    add_invesco_info = get_infos_from_yahoo(invesco_df, ex_isin_info)
    if len(add_invesco_info) > 0:
        logger.info("New data found")
        ex_isin_info = pd.concat([ex_isin_info, add_invesco_info])
    # After checking for new data, we can adjust the ISIN column
    invesco_df["ISIN"] = invesco_df["ISIN"].fillna(invesco_df["Name"])
    ex_isin_info.to_csv(
        "./data/isin_information.csv", index=False, sep=",", encoding="utf-8", mode="w"
    )
    ex_isin_info["Emittententicker"] = ex_isin_info["Symbol"].str.split(".").str[0]
    validate_editor(etf_handler, invesco_df["Wert"].sum(), "invesco")
    assert len(invesco_df) == len(invesco_df["ISIN"].unique())

    # Merge Amundi and Invesco
    merged_isin = amundi_merged.merge(
        invesco_df[["Name", "ISIN", "Wert"]], on="ISIN", how="outer"
    )
    merge_cols = ["Emittententicker", "Standort"]
    merged_isin = prepare_data_by_isin(merged_isin, ex_isin_info, merge_cols)
    assert (
        abs(
            invesco_df["Wert"].sum()
            + amundi_merged["Wert"].sum()
            - merged_isin["Wert"].sum()
        )
        < 0.1
    )
    ishares_merged = merge_same_editors(
        [etf.zusammensetzung for etf in etf_handler.etfs if etf.editor == "iShares"],
        ["Emittententicker", "Name", "Sektor", "Standort"],
        ["Emittententicker", "Name", "Sektor", "Standort", "Wert"],
    )
    ishares_merged = sum_and_replace(ishares_merged, "Wert")
    ishares_merged["Emittententicker"] = ishares_merged["Emittententicker"].fillna(
        ishares_merged["Name"]
    )
    ishares_merged["Emittententicker"] = ishares_merged["Emittententicker"].str.replace(
        " ", "-"
    )
    validate_ishare(ishares_merged, etf_handler)

    merged_df = ishares_merged.merge(merged_isin, on=merge_cols, how="outer")
    merged_df = prepare_data_by_ticker(merged_df)
    assert (
        abs(
            merged_df["Wert"].sum() - sum([etf.total_value for etf in etf_handler.etfs])
        )
        < 1
    )

    # Prepare Aktien
    aktien_depot = prepare_single_type(depot, "aktie")
    krypto_depot = prepare_single_type(depot, "krypto")

    depot_merged = pd.concat([merged_df, aktien_depot, krypto_depot], axis=0)

    assert (
        abs(
            depot_merged["Wert"].sum()
            - depot[depot["type"].isin(["etf", "aktie", "krypto"])]["Wert"].sum()
        )
        < 1
    )
    depot_merged["Name"] = depot_merged.groupby(
        ["Emittententicker", "Sektor", "Standort"]
    )["Name"].transform("first")
    depot_merged.to_csv(sink_path, index=False, sep=",", encoding="utf-8", mode="w")


if __name__ == "__main__":
    eval_date = "06.11.2024"

    ticker_config = {
        "A2DVB9": {
            "editor": "iShares",
            "url": "https://www.ishares.com/de/privatanleger/de/produkte/290846/fund/1478358465952.ajax?fileType=csv&fileName=2B7K_holdings&dataType=fund",
            "file_path": pathlib.Path("./data/msci_world_sri_zusammensetzung.csv"),
        },
        "A142N1": {
            "editor": "iShares",
            "url": "https://www.ishares.com/de/privatanleger/de/produkte/280510/ishares-sp-500-information-technology-sector-ucits-etf/1478358465952.ajax?fileType=csv&fileName=QDVE_holdings&dataType=fund",
            "file_path": pathlib.Path("./data/information-technology-sector.csv"),
        },
        "A2JSDC": {
            "editor": "amundi",
            "file_path": pathlib.Path(
                "./data/Fondszusammensetzung_Amundi Index MSCI Europe.csv"
            ),
        },
        "ETF908": {
            "editor": "amundi",
            "file_path": pathlib.Path("./data/Fondszusammensetzung_Amundi TecDAX.csv"),
        },
        "A3DSTC": {
            "editor": "amundi",
            "file_path": pathlib.Path(
                "./data/Fondszusammensetzung_Amundi S&P Global Health Care.csv"
            ),
        },
        "801498": {
            "editor": "invesco",
            "file_path": pathlib.Path(
                "./data/Die_10_größten_Positionen-holdings-2.xlsx"
            ),
        },
    }
    main(
        eval_date,
        "./data/depot.csv",
        "./data/isin_information.csv",
        "./data/depot_merged.csv",
        ticker_config,
    )
