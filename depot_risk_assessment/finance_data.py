import logging

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

_eur_rate: float | None = None


def _get_eur_rate() -> float:
    global _eur_rate
    if _eur_rate is None:
        info = yf.Ticker("EUR=X").get_info()
        _eur_rate = info.get("open", info.get("previousClose", 1.0))
        logger.info(f"Cached EUR/USD rate: {_eur_rate}")
    return _eur_rate


def get_ticker_info(ticker: str) -> dict:
    logger.debug(f"Getting info for {ticker}")
    info = yf.Ticker(ticker).get_info()
    logger.debug(f"Info for {ticker} received")
    return info


def get_info_for(ticker: str) -> dict[str, str | float]:
    try:
        info = get_ticker_info(ticker)
    except Exception as e:
        logger.exception(f"Failed to get info for {ticker}: {e}")
        return {"Price": float("nan"), "Sektor": None, "Standort": None}

    price = info.get("open", info.get("previousClose", 0))
    logger.info(f"Price for {ticker} is {price}")

    if info.get("currency") == "USD":
        logger.info("Currency is USD, converting to EUR")
        price = price * _get_eur_rate()

    return {
        "Price": price,
        "Sektor": info.get("sector", None),
        "Standort": info.get("country", None),
    }


def get_infos_for(tickers: list[str]) -> pd.DataFrame:
    infos = []
    for ticker in tickers:
        info = get_info_for(ticker)
        infos.append(info)
    return pd.DataFrame(infos)
