import logging

import numpy as np
import pandas as pd
import yahooquery as yq

from depot_risk_assessment.finance_data import get_ticker_info

logger = logging.getLogger(__name__)


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
        logger.error(f"Getting info from yahoo for {quote} failed with Key Error: {e}")
        return None
    except IndexError as e:
        logger.error(f"Getting info from yahoo for {quote} failed with {e}")
        return None


def get_infos_from_yahoo(df: pd.DataFrame, ex_info: pd.DataFrame) -> pd.DataFrame:
    new_data = []
    try:
        for i in range(len(df)):
            stock_isin = df["ISIN"].iloc[i]
            name = df["Name"].iloc[i]
            if stock_isin is np.nan:
                continue
            if stock_isin in ex_info["ISIN"].values:
                continue

            y_info = get_info_from_yahoo(stock_isin)
            if y_info is None:
                logger.info(f"Getting info for {stock_isin} did return None. Trying with name {name}")
                y_info = get_info_from_yahoo(name)
            if y_info is None:
                logger.info(f"Getting info for {stock_isin} with name {name} did return None")
                continue
            y_info["ISIN"] = stock_isin
            y_info["Name"] = name
            new_data.append(y_info)
        return pd.DataFrame(new_data)
    except Exception as e:
        logger.exception(f"Error during ISIN resolution: {e}")
        return pd.DataFrame(new_data)
