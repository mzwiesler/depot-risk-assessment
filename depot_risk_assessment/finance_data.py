import logging
from functools import reduce

import numpy as np
import pandas as pd
import yahooquery as yq
import yfinance as yf

logger = logging.getLogger(__name__)


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


def get_infos_for(tickers: list[str]) -> pd.DataFrame:
    infos = []
    for ticker in tickers:
        info = get_info_for(ticker)
        infos.append(info)
    return pd.DataFrame(infos)


def get_info_for(ticker: str) -> dict[str, str | float]:
    info = get_ticker_info(ticker)
    result = {}
    price = info.get("open", info.get("previousClose", 0))
    logger.info(f"Price for {ticker} is {price}")
    if info.get("currency") == "USD":
        logger.info("Currency is USD")
        eur = get_ticker_info("EUR=X")
        price = price * eur["open"]
    result["Price"] = price
    result["Sektor"] = info.get("sector", None)
    result["Standort"] = info.get("country", None)

    return result


def validate_wert_for(wert: pd.Series, editor: str, total_value: pd.Series) -> None:
    assert (
        abs(
            wert.sum()
            - sum(
                [etf["total_value"] for etf in etfs.values() if etf["editor"] == editor]
            )
        )
        < 0.2
    )
