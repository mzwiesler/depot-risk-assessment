"""Tests for ETF provider read/normalize pipelines."""

import pathlib

from depot_risk_assessment.providers.amundi import AmundiProvider
from depot_risk_assessment.providers.hanetf import HanetfProvider
from depot_risk_assessment.providers.invesco import InvescoProvider
from depot_risk_assessment.providers.ishares import ISharesProvider
from depot_risk_assessment.providers.xtrackers import XtrackersProvider


def _assert_normalized(df, total_value=1000.0):
    """Common assertions that apply to every normalized provider output."""
    for col in ["Name", "Gewichtung", "Wert"]:
        assert col in df.columns, f"Missing column: {col}"
    assert abs(df["Gewichtung"].sum() - 100.0) < 0.01, f"Gewichtung does not sum to 100: {df['Gewichtung'].sum()}"
    assert abs(df["Wert"].sum() - total_value) < 0.01, f"Wert does not sum to {total_value}: {df['Wert'].sum()}"
    assert (df["Gewichtung"] != 0).all(), "Found zero-weight rows"


class TestAmundiProvider:
    def _write_fixture(self, path: pathlib.Path) -> pathlib.Path:
        """Write a minimal Amundi CSV (19 skip lines then header + data)."""
        csv_path = path / "amundi_test.csv"
        skip_lines = "\n".join([f"skip{i}" for i in range(19)])
        # header row + 2 data rows
        content = (
            skip_lines
            + "\n"
            + ",Name,Anlageklasse,Gewichtung,Land,Sektor\n"
            + "0,Apple Inc,Aktie,60.0,United States,Technology\n"
            + "1,Microsoft Corp,Aktie,40.0,United States,Technology\n"
        )
        csv_path.write_text(content)
        return csv_path

    def test_normalize_output_columns(self, tmp_path, sector_mapping):
        provider = AmundiProvider()
        csv_path = self._write_fixture(tmp_path)
        df = provider.read(csv_path)
        result = provider.normalize(df, total_value=1000.0, sector_mapping=sector_mapping)
        _assert_normalized(result)

    def test_normalize_gewichtung_sums_to_100(self, tmp_path, sector_mapping):
        provider = AmundiProvider()
        csv_path = self._write_fixture(tmp_path)
        df = provider.read(csv_path)
        result = provider.normalize(df, total_value=1000.0, sector_mapping=sector_mapping)
        assert abs(result["Gewichtung"].sum() - 100.0) < 0.01

    def test_normalize_wert_sums_to_total_value(self, tmp_path, sector_mapping):
        provider = AmundiProvider()
        csv_path = self._write_fixture(tmp_path)
        df = provider.read(csv_path)
        result = provider.normalize(df, total_value=1000.0, sector_mapping=sector_mapping)
        assert abs(result["Wert"].sum() - 1000.0) < 0.01

    def test_normalize_no_zero_gewichtung(self, tmp_path, sector_mapping):
        provider = AmundiProvider()
        csv_path = self._write_fixture(tmp_path)
        df = provider.read(csv_path)
        result = provider.normalize(df, total_value=1000.0, sector_mapping=sector_mapping)
        assert (result["Gewichtung"] != 0).all()

    def test_normalize_renames_land_to_standort(self, tmp_path, sector_mapping):
        provider = AmundiProvider()
        csv_path = self._write_fixture(tmp_path)
        df = provider.read(csv_path)
        result = provider.normalize(df, total_value=1000.0, sector_mapping=sector_mapping)
        assert "Standort" in result.columns
        assert "Land" not in result.columns

    def test_normalize_sector_mapping_applied(self, tmp_path, sector_mapping):
        provider = AmundiProvider()
        csv_path = self._write_fixture(tmp_path)
        df = provider.read(csv_path)
        result = provider.normalize(df, total_value=1000.0, sector_mapping=sector_mapping)
        assert set(result["Sektor"].dropna().unique()).issubset(set(sector_mapping.values()))


class TestInvescoProvider:
    def _write_fixture(self, path: pathlib.Path) -> pathlib.Path:
        """Write a minimal Invesco CSV.

        pd.read_csv(header=1, skiprows=4):
        - skiprows=4 skips lines 0-3
        - header=1 means the 2nd row of what remains (index 1) is the header
        So we need: 4 skip lines, 1 dummy row (index 0), then the real header, then data.
        """
        csv_path = path / "invesco_test.csv"
        lines = [
            "skip1",
            "skip2",
            "skip3",
            "skip4",
            "dummy_row",  # row index 0 after skiprows, ignored by header=1
            "Full name,Weight,ISIN",  # row index 1 → becomes header
            "Apple Inc,60.0,US0378331005",
            "Microsoft Corp,40.0,US5949181045",
        ]
        csv_path.write_text("\n".join(lines) + "\n")
        return csv_path

    def test_normalize_output(self, tmp_path, sector_mapping):
        provider = InvescoProvider()
        csv_path = self._write_fixture(tmp_path)
        df = provider.read(csv_path)
        result = provider.normalize(df, total_value=1000.0, sector_mapping=sector_mapping)
        _assert_normalized(result)

    def test_normalize_gewichtung_sums_to_100(self, tmp_path, sector_mapping):
        provider = InvescoProvider()
        csv_path = self._write_fixture(tmp_path)
        df = provider.read(csv_path)
        result = provider.normalize(df, total_value=1000.0, sector_mapping=sector_mapping)
        assert abs(result["Gewichtung"].sum() - 100.0) < 0.01

    def test_normalize_wert_sums_to_total_value(self, tmp_path, sector_mapping):
        provider = InvescoProvider()
        csv_path = self._write_fixture(tmp_path)
        df = provider.read(csv_path)
        result = provider.normalize(df, total_value=1000.0, sector_mapping=sector_mapping)
        assert abs(result["Wert"].sum() - 1000.0) < 0.01

    def test_normalize_no_zero_gewichtung(self, tmp_path, sector_mapping):
        provider = InvescoProvider()
        csv_path = self._write_fixture(tmp_path)
        df = provider.read(csv_path)
        result = provider.normalize(df, total_value=1000.0, sector_mapping=sector_mapping)
        assert (result["Gewichtung"] != 0).all()


class TestHanetfProvider:
    def _write_fixture(self, path: pathlib.Path) -> pathlib.Path:
        """Write a minimal HANetf CSV (3 skip lines then header + data)."""
        csv_path = path / "hanetf_test.csv"
        lines = [
            "skip1",
            "skip2",
            "skip3",
            "Security Description,Weight,ISIN",
            "Apple Inc,60.0,US0378331005",
            "Microsoft Corp,40.0,US5949181045",
        ]
        csv_path.write_text("\n".join(lines) + "\n")
        return csv_path

    def test_normalize_output(self, tmp_path, sector_mapping):
        provider = HanetfProvider()
        csv_path = self._write_fixture(tmp_path)
        df = provider.read(csv_path)
        result = provider.normalize(df, total_value=1000.0, sector_mapping=sector_mapping)
        _assert_normalized(result)

    def test_normalize_gewichtung_sums_to_100(self, tmp_path, sector_mapping):
        provider = HanetfProvider()
        csv_path = self._write_fixture(tmp_path)
        df = provider.read(csv_path)
        result = provider.normalize(df, total_value=1000.0, sector_mapping=sector_mapping)
        assert abs(result["Gewichtung"].sum() - 100.0) < 0.01

    def test_normalize_wert_sums_to_total_value(self, tmp_path, sector_mapping):
        provider = HanetfProvider()
        csv_path = self._write_fixture(tmp_path)
        df = provider.read(csv_path)
        result = provider.normalize(df, total_value=1000.0, sector_mapping=sector_mapping)
        assert abs(result["Wert"].sum() - 1000.0) < 0.01

    def test_normalize_no_zero_gewichtung(self, tmp_path, sector_mapping):
        provider = HanetfProvider()
        csv_path = self._write_fixture(tmp_path)
        df = provider.read(csv_path)
        result = provider.normalize(df, total_value=1000.0, sector_mapping=sector_mapping)
        assert (result["Gewichtung"] != 0).all()

    def test_normalize_renames_security_description_to_name(self, tmp_path, sector_mapping):
        provider = HanetfProvider()
        csv_path = self._write_fixture(tmp_path)
        df = provider.read(csv_path)
        result = provider.normalize(df, total_value=1000.0, sector_mapping=sector_mapping)
        assert "Name" in result.columns


class TestXtrackersProvider:
    def _write_fixture(self, path: pathlib.Path) -> pathlib.Path:
        """Write a minimal Xtrackers CSV (3 skip lines then header + data)."""
        csv_path = path / "xtrackers_test.csv"
        lines = [
            "skip1",
            "skip2",
            "skip3",
            "Name,Weighting,ISIN",
            "Apple Inc,60.0,US0378331005",
            "Microsoft Corp,40.0,US5949181045",
        ]
        csv_path.write_text("\n".join(lines) + "\n")
        return csv_path

    def test_normalize_output(self, tmp_path, sector_mapping):
        provider = XtrackersProvider()
        csv_path = self._write_fixture(tmp_path)
        df = provider.read(csv_path)
        result = provider.normalize(df, total_value=1000.0, sector_mapping=sector_mapping)
        _assert_normalized(result)

    def test_normalize_gewichtung_sums_to_100(self, tmp_path, sector_mapping):
        provider = XtrackersProvider()
        csv_path = self._write_fixture(tmp_path)
        df = provider.read(csv_path)
        result = provider.normalize(df, total_value=1000.0, sector_mapping=sector_mapping)
        assert abs(result["Gewichtung"].sum() - 100.0) < 0.01

    def test_normalize_wert_sums_to_total_value(self, tmp_path, sector_mapping):
        provider = XtrackersProvider()
        csv_path = self._write_fixture(tmp_path)
        df = provider.read(csv_path)
        result = provider.normalize(df, total_value=1000.0, sector_mapping=sector_mapping)
        assert abs(result["Wert"].sum() - 1000.0) < 0.01

    def test_normalize_no_zero_gewichtung(self, tmp_path, sector_mapping):
        provider = XtrackersProvider()
        csv_path = self._write_fixture(tmp_path)
        df = provider.read(csv_path)
        result = provider.normalize(df, total_value=1000.0, sector_mapping=sector_mapping)
        assert (result["Gewichtung"] != 0).all()

    def test_normalize_renames_weighting_to_gewichtung(self, tmp_path, sector_mapping):
        provider = XtrackersProvider()
        csv_path = self._write_fixture(tmp_path)
        df = provider.read(csv_path)
        result = provider.normalize(df, total_value=1000.0, sector_mapping=sector_mapping)
        assert "Gewichtung" in result.columns
        assert "Weighting" not in result.columns


class TestISharesProvider:
    def _write_fixture(self, path: pathlib.Path) -> pathlib.Path:
        """Write a minimal iShares CSV (2 skip lines then header + data).

        Gewichtung (%) values must be strings with comma decimals (e.g. '30,5').
        """
        csv_path = path / "ishares_test.csv"
        lines = [
            "skip1",
            "skip2",
            "Name,Gewichtung (%),Sektor,Standort,Marktwährung",
            'Apple Inc,"60,0",Technology,United States,USD',
            'Microsoft Corp,"40,0",Technology,United States,USD',
        ]
        csv_path.write_text("\n".join(lines) + "\n")
        return csv_path

    def test_normalize_output(self, tmp_path, sector_mapping):
        provider = ISharesProvider()
        csv_path = self._write_fixture(tmp_path)
        df = provider.read(csv_path)
        result = provider.normalize(df, total_value=1000.0, sector_mapping=sector_mapping)
        _assert_normalized(result)

    def test_normalize_gewichtung_sums_to_100(self, tmp_path, sector_mapping):
        provider = ISharesProvider()
        csv_path = self._write_fixture(tmp_path)
        df = provider.read(csv_path)
        result = provider.normalize(df, total_value=1000.0, sector_mapping=sector_mapping)
        assert abs(result["Gewichtung"].sum() - 100.0) < 0.01

    def test_normalize_wert_sums_to_total_value(self, tmp_path, sector_mapping):
        provider = ISharesProvider()
        csv_path = self._write_fixture(tmp_path)
        df = provider.read(csv_path)
        result = provider.normalize(df, total_value=1000.0, sector_mapping=sector_mapping)
        assert abs(result["Wert"].sum() - 1000.0) < 0.01

    def test_normalize_no_zero_gewichtung(self, tmp_path, sector_mapping):
        provider = ISharesProvider()
        csv_path = self._write_fixture(tmp_path)
        df = provider.read(csv_path)
        result = provider.normalize(df, total_value=1000.0, sector_mapping=sector_mapping)
        assert (result["Gewichtung"] != 0).all()

    def test_normalize_renames_marktwehrung_to_wahrung(self, tmp_path, sector_mapping):
        provider = ISharesProvider()
        csv_path = self._write_fixture(tmp_path)
        df = provider.read(csv_path)
        result = provider.normalize(df, total_value=1000.0, sector_mapping=sector_mapping)
        assert "Marktwährung" not in result.columns
        assert "Währung" in result.columns
