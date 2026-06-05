import tomllib
from pathlib import Path

_MAPPINGS_PATH = Path(__file__).parent.parent / "data" / "mappings.toml"


def _load() -> dict:
    with open(_MAPPINGS_PATH, "rb") as f:
        return tomllib.load(f)


def _parse_mapping(raw: dict[str, str]) -> dict[str | None, str | None]:
    result: dict[str | None, str | None] = {}
    for key, value in raw.items():
        parsed_value = value if value != "" else None
        result[key] = parsed_value
    return result


_data = _load()

sector_mapping: dict[str | None, str | None] = _parse_mapping(_data["sector_mapping"])
sector_mapping_yahoo: dict[str | None, str | None] = _parse_mapping(_data["sector_mapping_yahoo"])
sector_mapping_yahoo[None] = None

country_mapping_yahoo: dict[str | None, str | None] = _parse_mapping(_data["country_mapping_yahoo"])
country_mapping_ishare: dict[str, str] = _data["country_mapping_ishare"]
