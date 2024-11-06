import logging
import pathlib
import re
import unicodedata
from functools import reduce

import numpy as np
import pandas as pd
import requests
import yahooquery as yq
import yfinance as yf
from cleanco import basename

logger = logging.getLogger(__name__)


def setup_root_logger(level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger()
    stream_handler = logging.StreamHandler()
    logger.setLevel(level)
    logger.handlers = [stream_handler]
    return logger


def download_zusammensetzung_as_csv(url: str, file_path: pathlib.Path) -> None:
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        with open(file_path, "wb") as file:
            file.write(response.content)
        print("CSV file downloaded successfully.")
    else:
        print(f"Failed to download CSV file. Status code: {response.status_code}")


def read_ishare_from(file_path: pathlib.Path) -> pd.DataFrame:
    return pd.read_csv(file_path, header="infer", sep=",", skiprows=2)


def read_invesco_xlsx(file_path: pathlib.Path) -> pd.DataFrame:
    return pd.read_excel(file_path, header=1, skiprows=4)


def read_amundi_from(file_path: pathlib.Path) -> pd.DataFrame:
    df = pd.read_csv(file_path, header="infer", skiprows=19, sep=";")
    df = df[~df["Gewichtung"].isna()]
    return df


def get_ticker_info(ticker: str) -> dict:
    try:
        logger.debug(f"Getting info for {ticker}")
        info = yf.Ticker(ticker).info
        logger.debug(f"Info for {ticker} received")
    except TimeoutError:
        logger.error(f"Timeout error for {ticker}")
        info = yf.Ticker(ticker).info

    return info


def get_info_from_yahoo(quote: str) -> dict | None:
    try:
        symbol = yq.search(quote)["quotes"][0]["symbol"]
        info = get_ticker_info(symbol)
        return {
            "Symbol": symbol,
            "Sektor": info.get("sector", None),
            "Standort": info.get("country", None),
        }
    except KeyError as e:
        print(f"Key error: {e}")
        return None
    except IndexError as e:
        print(e)
        return None


def get_infos_from_yahoo(df: pd.DataFrame, ex_info: pd.DataFrame) -> pd.DataFrame:
    new_data = []
    try:
        for i in range(len(df)):
            stock_isin = df["ISIN"][i]
            name = df["Name"][i]
            if stock_isin is np.nan:
                continue
            if stock_isin in ex_info["ISIN"].values:
                continue
            print(stock_isin)
            print(name)
            y_info = get_info_from_yahoo(stock_isin)
            if y_info is None:
                y_info = get_info_from_yahoo(name)
            if y_info is None:
                continue
            y_info["ISIN"] = stock_isin
            y_info["Name"] = name
            new_data.append(y_info)
        return pd.DataFrame(new_data)
    except Exception as e:
        print(e)
        return pd.DataFrame(new_data)


def rescale(col: pd.Series) -> pd.Series:
    return round(100 * col / sum(col), 8)


def join_on_first_word(
    df1: pd.DataFrame, df2: pd.DataFrame, column: str
) -> pd.DataFrame:
    df1["first_word"] = df1[column].str.split(" ").str[0]
    df2["first_word"] = df2[column].str.split(" ").str[0]
    joined_df = df1.merge(df2, on="first_word", how="outer", suffixes=("_1", "_2"))
    return joined_df


def prepare_company_name(col: pd.Series) -> pd.Series:
    col_lower = col.str.lower()
    col_encoded = col_lower.apply(
        lambda x: unicodedata.normalize("NFKD", x).encode("ASCII", "ignore").decode()
    )
    # substitute dash with space
    col_re = col_encoded.apply(lambda x: re.sub(r"-", " ", x))
    col_re = col_re.apply(lambda x: re.sub(r"[^\w\s]", "", x))
    col_base = col_re.apply(lambda x: basename(x))
    return col_base


def aggregate_gewichtung_by(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    return df.groupby(cols).agg({"Gewichtung": "sum"}).reset_index()


def sum_duplicates_by(
    df: pd.DataFrame, col_sum: str, cols_group: list[str]
) -> pd.DataFrame:
    df_grouped = df.groupby(cols_group, dropna=False).agg({col_sum: "sum"})
    df = df.drop_duplicates(subset=cols_group)
    df = df.drop(columns=[col_sum])
    result = pd.merge(df, df_grouped, on=cols_group)
    return result


def prepare_amundi_data(
    df: pd.DataFrame,
    sector_mapping: dict[str, str],
    value: float,
) -> pd.DataFrame:
    # Cleaning
    df = df.drop(columns="Unnamed: 0")
    # if name is nan, fill with Anlageklasse
    df["Name"] = df["Name"].fillna(df["Anlageklasse"])
    df["Gewichtung"] = (
        df["Gewichtung"].str.replace("%", "").str.replace(",", ".").astype(float)
    )
    df = df[df["Gewichtung"] != 0]
    df = df.rename(columns={"Land": "Standort"})
    # standardize sector and land to be mergable with other data
    df["Sektor"] = df["Sektor"].map(sector_mapping)
    # df["Standort"] = df["Land"].apply(
    #     lambda x: land_mapping[x] if x in land_mapping else x
    # )

    # df["Name_prep"] = prepare_company_name(df["Name"])
    # # Solve one character words (example: 'l oreal' -> 'loreal')
    # df["Name_prep"] = df["Name_prep"].apply(
    #     lambda x: x[0] + x[2:] if len(x.split(" ")[0]) == 1 else x
    # )

    # df["first_word"] = df["Name_prep"].str.split(" ").str[0]

    # df = sum_duplicates_by_gewichtung(df, merge_cols)
    df["Gewichtung"] = rescale(df["Gewichtung"])
    df["Wert"] = round(df["Gewichtung"] * value / 100, 2)
    return df


def prepare_invesco_data(df: pd.DataFrame, value: float) -> pd.DataFrame:
    df = df[~df["Weight"].isna()]
    df = df.rename(columns={"Full name": "Name", "Weight": "Gewichtung"})
    df["Name"] = df["Name"].str.split("USD").str[0].str.strip()
    # After checking for new data, we can adjust the ISIN column
    df["ISIN"] = df["ISIN"].fillna(df["Name"])
    df["Gewichtung"] = rescale(df["Gewichtung"])
    df["Wert"] = round(df["Gewichtung"] * value / 100, 2)
    return df


def prepare_ishare_data(df: pd.DataFrame, value: float) -> pd.DataFrame:
    df = df.rename(columns={"Gewichtung (%)": "Gewichtung", "Marktwährung": "Währung"})
    # filter for name not null
    df = df[df["Name"].notnull()]
    # df["Name_prep"] = prepare_company_name(df["Name"])
    # Remove trailing blanks from Sektor
    df["Sektor"] = df["Sektor"].str.strip()
    # Gewichtung to float
    df["Gewichtung"] = (
        df["Gewichtung"].str.replace("%", "").str.replace(",", ".").astype(float)
    )
    # Gewichtung > 0
    df = df[df["Gewichtung"] != 0]
    # First word of Name_prep
    # df["first_word"] = df["Name_prep"].str.split(" ").str[0]

    # df = sum_duplicates_by(df, "Gewichtung", merge_cols)
    df["Gewichtung"] = rescale(df["Gewichtung"])
    df["Wert"] = round(df["Gewichtung"] * value / 100, 2)
    return df


# def get_total_value_of(
#     config: dict[str, Any], depot: pd.DataFrame, date: str
# ) -> pd.DataFrame:
#     eur = get_ticker_info("EUR=X")
#     etf_infos_open = {}
#     for key in config.keys():
#         ticker = depot[depot["wkn"] == key]["ticker"].values[0]
#         info = get_ticker_info(ticker)
#         price = (
#             info["open"] if info["currency"] != "USD" else info["open"] * eur["open"]
#         )
#         num_shares = depot[depot["wkn"] == key][date].values[0]
#         etf_info = {}
#         etf_value_open = price * num_shares
#         etf_info["price"] = price
#         etf_info["num_shares"] = num_shares
#         etf_info["value_open"] = etf_value_open
#         etf_infos_open[key] = etf_info
#     total_value = sum([value["value_open"] for value in etf_infos_open.values()])
#     etf_infos_open["sum"] = {"price": 0, "num_shares": 0, "value_open": total_value}
#     etf_infos = pd.DataFrame.from_dict(etf_infos_open, orient="index")

#     # Calculate the percentage of the total value
#     etf_infos["percentage"] = etf_infos["value_open"] / total_value * 100
#     return etf_infos


def get_info_for(row: pd.Series) -> pd.Series:
    info = get_ticker_info(row.loc["ticker"])
    result = {}
    price = info.get("open", info.get("previousClose", 0))
    logger.info(f"Price for {row.loc['ticker']} is {price}")
    if info.get("currency") == "USD":
        logger.info("Currency is USD")
        eur = get_ticker_info("EUR=X")
        price = price * eur["open"]
    result["Price"] = price
    result["Sektor"] = info.get("sector", None)
    result["Standort"] = info.get("country", None)

    return pd.Series(result)


def merge_same_editors(
    dfs: list[pd.DataFrame], merge_cols: list[str], col_to_keep: list[str]
) -> pd.DataFrame:
    dfs = [df[col_to_keep] for df in dfs]
    # merge all dataframes in list on merge_cols using functools
    df = reduce(lambda x, y: x.merge(y, on=merge_cols, how="outer"), dfs)
    return df


def sum_and_replace(df: pd.DataFrame, col_contains: str) -> pd.DataFrame:
    col_names_containing = [col for col in df.columns if col_contains in col]
    df[col_names_containing] = df[col_names_containing].fillna(0)
    row_sum = df[col_names_containing].sum(axis=1)
    df = df.drop(columns=col_names_containing)
    df[col_contains] = row_sum
    return df


def merge_and_drop_col(
    df: pd.DataFrame, col1: str, col2: str, new_col: str
) -> pd.DataFrame:
    df[new_col] = df[col1].fillna(df[col2])
    df = df.drop(columns=[col1, col2])
    return df
