import logging
import pathlib
import typing
from dataclasses import dataclass

import pandas as pd
import yfinance as yf

from depot_risk_assessment.functions import (
    download_zusammensetzung_as_csv,
    get_info_for,
    get_infos_from_yahoo,
    merge_and_drop_col,
    merge_same_editors,
    prepare_amundi_data,
    prepare_invesco_data,
    prepare_ishare_data,
    read_amundi_from,
    read_invesco_xlsx,
    read_ishare_from,
    setup_root_logger,
    sum_and_replace,
    sum_duplicates_by,
)
from depot_risk_assessment.mapping import (
    country_mapping_ishare,
    country_mapping_yahoo,
    sector_mapping,
    sektor_mapping_yahoo,
)

logger = logging.getLogger(__name__)


def get_etf_info(ticker_config: dict, depot: pd.DataFrame):
    etfs = ticker_config.copy()
    for key, value in ticker_config.items():
        total_value_etf = depot[depot["wkn"] == key]["Wert"].values[0]
        if value["editor"] == "iShares":
            url = value["url"]
            path = value["file_path"]
            download_zusammensetzung_as_csv(url, path)
            df = read_ishare_from(path)
            df = prepare_ishare_data(df, total_value_etf)
        elif value["editor"] == "amundi":
            path = value["file_path"]
            df = read_amundi_from(path)
            df = prepare_amundi_data(df, sector_mapping, total_value_etf)
        else:
            path = value["file_path"]
            df = read_invesco_xlsx(path)
            df = prepare_invesco_data(df, total_value_etf)
        etfs[key]["zusammensetzung"] = df
        etfs[key]["total_value"] = total_value_etf
        etfs[key]["editor"] = value["editor"]
    return etfs


def main(
    eval_date: str,
    path_to_depot: str,
    path_to_isin_info: str,
    sink_path: str,
    ticker_config: dict,
):
    logger.error("This is an error")
    logger.info("This is an info")
    depot = pd.read_csv(pathlib.Path(path_to_depot), header="infer", sep=";")
    depot = pd.concat([depot, depot.apply(get_info_for, axis=1)], axis=1)
    depot["Wert"] = depot["Price"] * depot[eval_date]
    depot["Percentage"] = depot["Wert"] / depot["Wert"].sum() * 100
    print(f"Total value: {depot['Wert'].sum()}")
    # depot.groupby("type").agg({"Wert": "sum", "Percentage": "sum"})
    # After checking for new data, we can adjust the ISIN column
    etfs = get_etf_info(ticker_config, depot)
    depot[depot["type"] == "etf"]["Wert"].sum()
    assert (
        abs(
            sum([etf["total_value"] for _, etf in etfs.items()])
            - depot[depot["type"] == "etf"]["Wert"].sum()
        )
        < 0.01
    )
    ex_isin_info = pd.read_csv(pathlib.Path(path_to_isin_info), header="infer", sep=",")
    amundi_merged = merge_same_editors(
        [etf["zusammensetzung"] for etf in etfs.values() if etf["editor"] == "amundi"],
        ["ISIN", "Name", "Sektor", "Standort"],
        ["ISIN", "Name", "Sektor", "Standort", "Wert"],
    )
    amundi_merged = sum_and_replace(amundi_merged, "Wert")

    # assert almost equal
    assert (
        abs(
            amundi_merged["Wert"].sum()
            - sum(
                [
                    etf["total_value"]
                    for etf in etfs.values()
                    if etf["editor"] == "amundi"
                ]
            )
        )
        < 0.1
    )
    add_amundi_info = get_infos_from_yahoo(amundi_merged, ex_isin_info)
    if len(add_amundi_info) > 0:
        print("New data found")
        ex_isin_info = pd.concat([ex_isin_info, add_amundi_info])

    # After checking for new data, we can adjust the ISIN column
    amundi_merged["ISIN"] = amundi_merged["ISIN"].fillna(amundi_merged["Name"])
    assert len(amundi_merged) == len(amundi_merged["ISIN"].unique())
    invesco_df = [
        etf["zusammensetzung"] for etf in etfs.values() if etf["editor"] == "invesco"
    ][0]
    add_invesco_info = get_infos_from_yahoo(invesco_df, ex_isin_info)
    if len(add_invesco_info) > 0:
        print("New data found")
        ex_isin_info = pd.concat([ex_isin_info, add_invesco_info])
    # After checking for new data, we can adjust the ISIN column
    invesco_df["ISIN"] = invesco_df["ISIN"].fillna(invesco_df["Name"])
    ex_isin_info.to_csv(
        "./data/isin_information.csv", index=False, sep=",", encoding="utf-8", mode="w"
    )
    ex_isin_info["Emittententicker"] = ex_isin_info["Symbol"].str.split(".").str[0]
    # assert almost equal
    assert (
        abs(
            invesco_df["Wert"].sum()
            - sum(
                [
                    etf["total_value"]
                    for etf in etfs.values()
                    if etf["editor"] == "invesco"
                ]
            )
        )
        < 0.1
    )
    assert len(invesco_df) == len(invesco_df["ISIN"].unique())
    merged_isin = amundi_merged.merge(
        invesco_df[["Name", "ISIN", "Wert"]], on="ISIN", how="outer"
    )
    merged_isin = sum_and_replace(merged_isin, "Wert")
    merged_isin = merge_and_drop_col(merged_isin, "Name_x", "Name_y", "Name")
    merged_isin = merged_isin.merge(ex_isin_info, on="ISIN", how="left")
    assert (
        abs(
            invesco_df["Wert"].sum()
            + amundi_merged["Wert"].sum()
            - merged_isin["Wert"].sum()
        )
        < 0.1
    )

    merged_isin["Standort_y"] = merged_isin["Standort_y"].map(country_mapping_yahoo)
    merged_isin = merge_and_drop_col(
        merged_isin, "Standort_x", "Standort_y", "Standort"
    )
    merged_isin["Sektor_y"] = merged_isin["Sektor_y"].map(sektor_mapping_yahoo)
    merged_isin = merge_and_drop_col(merged_isin, "Sektor_x", "Sektor_y", "Sektor")
    merged_isin = merge_and_drop_col(merged_isin, "Name_x", "Name_y", "Name")
    merge_cols = ["Emittententicker", "Standort"]
    merged_isin = sum_duplicates_by(merged_isin, "Wert", merge_cols)
    merged_isin = merged_isin.drop(columns=["ISIN", "Symbol"])
    assert (
        abs(
            invesco_df["Wert"].sum()
            + amundi_merged["Wert"].sum()
            - merged_isin["Wert"].sum()
        )
        < 0.1
    )
    merged_isin["Standort"] = merged_isin["Standort"].replace(country_mapping_ishare)
    merged_isin["Emittententicker"] = merged_isin["Emittententicker"].fillna(
        merged_isin["Name"]
    )
    ishares_merged = merge_same_editors(
        [etf["zusammensetzung"] for etf in etfs.values() if etf["editor"] == "iShares"],
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
    # assert almost equal
    assert (
        abs(
            ishares_merged["Wert"].sum()
            - sum(
                [
                    etf["total_value"]
                    for etf in etfs.values()
                    if etf["editor"] == "iShares"
                ]
            )
        )
        < 1
    )
    assert ishares_merged["Emittententicker"].isna().sum() == 0
    assert ishares_merged["Standort"].isna().sum() == 0
    assert ishares_merged["Wert"].isna().sum() == 0
    assert (
        ishares_merged.groupby(["Emittententicker", "Standort"]).agg(
            {"Standort": "count"}
        )
        > 1
    )["Standort"].sum() == 0
    merged_df = ishares_merged.merge(
        merged_isin, on=["Emittententicker", "Standort"], how="outer"
    )
    merged_df["Name"] = merged_df["Name_x"].fillna(merged_df["Name_y"])
    merged_df = sum_and_replace(merged_df, "Wert")
    merged_df = merge_and_drop_col(merged_df, "Name_x", "Name_y", "Name")
    merged_df = merge_and_drop_col(merged_df, "Sektor_x", "Sektor_y", "Sektor")
    df_grouped = merged_df.groupby("Name").agg({"Wert": "sum"})
    merged_df = merged_df.drop_duplicates(subset=["Name"]).drop(columns=["Wert"])
    merged_df = merged_df.merge(df_grouped, on="Name", how="left")
    multiple_ticker = merged_df.groupby("Emittententicker").agg(
        {"Name": "count", "Sektor": "count", "Standort": "count"}
    )
    multiple_ticker = multiple_ticker[multiple_ticker["Name"] > 1]
    merged_df[merged_df["Emittententicker"].isin(multiple_ticker.index)]
    merged_df[merged_df["Standort"].isna()]
    assert (
        abs(
            merged_df["Wert"].sum() - sum([etf["total_value"] for etf in etfs.values()])
        )
        < 1
    )
    merged_df["Type"] = "ETF"
    aktien_depot = depot[depot["type"] == "aktie"][
        ["info", "ticker", "Wert", "Standort", "Sektor"]
    ].copy()
    aktien_depot = aktien_depot.rename(
        columns={"info": "Name", "ticker": "Emittententicker"}
    )
    aktien_depot["Name"] = aktien_depot["Name"].str.upper()
    aktien_depot["Emittententicker"] = (
        aktien_depot["Emittententicker"].str.split(".").str[0]
    )
    aktien_depot["Standort"] = aktien_depot["Standort"].map(country_mapping_yahoo)
    aktien_depot["Standort"] = aktien_depot["Standort"].replace(country_mapping_ishare)
    aktien_depot["Sektor"] = aktien_depot["Sektor"].map(sektor_mapping_yahoo)
    aktien_depot["Type"] = "Aktie"

    krypto_depot = depot[depot["type"] == "krypto"][
        ["info", "ticker", "Wert", "Standort", "Sektor"]
    ].copy()
    krypto_depot = krypto_depot.rename(
        columns={"info": "Name", "ticker": "Emittententicker"}
    )
    krypto_depot["Name"] = krypto_depot["Name"].str.upper()
    krypto_depot["Emittententicker"] = (
        krypto_depot["Emittententicker"].str.split(".").str[0]
    )
    krypto_depot["Standort"] = krypto_depot["Standort"].map(country_mapping_yahoo)
    krypto_depot["Standort"] = krypto_depot["Standort"].replace(country_mapping_ishare)
    krypto_depot["Sektor"] = krypto_depot["Sektor"].map(sektor_mapping_yahoo)
    krypto_depot["Type"] = "Krypto"

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

    @dataclass
    class Ticker:
        editor: str
        url: str | None
        file_path: str

    @dataclass
    class TickerHandler:
        tickers: list[Ticker]

        @classmethod
        def from_dict(cls: typing.Type["TickerHandler"], data: dict) -> "TickerHandler":
            tickers = [Ticker(**ticker) for ticker in data]
            return TickerHandler(tickers=tickers)

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
