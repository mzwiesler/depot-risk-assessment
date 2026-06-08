from typing import TypedDict


class NormalizedHolding(TypedDict):
    Name: str
    ISIN: str | None
    Emittententicker: str | None
    Sektor: str | None
    Standort: str | None
    Gewichtung: float
    Wert: float


class MergedPosition(TypedDict):
    Emittententicker: str
    Standort: str | None
    Name: str
    Sektor: str | None
    Wert: float
    Type: str


ISHARES_EDITOR = "iShares"
