import zipfile
import pandas as pd
from pathlib import Path
from openpyxl import load_workbook
import logging

logger = logging.getLogger(__name__)


def read_xlsx_file_without_styles(file_path: str) -> pd.DataFrame:
    """
    Reads the holdings Excel file from the downloads folder.

    Args:
        file_path: Optional path to the Excel file. If None, uses default path.

    Returns:
        pd.DataFrame: DataFrame containing the holdings data
    """
    src = Path(file_path)

    if not src.exists():
        raise FileNotFoundError(f"File not found: {src}")

    dst = src.with_name("nostyles.xlsx")

    with zipfile.ZipFile(src, "r") as zin:
        with zipfile.ZipFile(dst, "w") as zout:
            for item in zin.infolist():
                # skip the stylesheet
                if item.filename != "xl/styles.xml":
                    zout.writestr(item, zin.read(item.filename))

    wb = load_workbook(
        dst,
        read_only=True,
        data_only=True,
    )

    ws = wb.active

    data = [[cell.value for cell in row] for row in ws.iter_rows()]

    print(f"✓ Successfully read {len(data)} rows from {src.name}")
    # print(f"\nColumns: {list(data.columns)}")
    # print(f"\nFirst few rows:")
    # print(df.head())

    return pd.DataFrame(data)


if __name__ == "__main__":
    holdings_df = read_xlsx_file_without_styles(
        "downloads/Fondszusammensetzung_Amundi S&P World Health Care Screened UCITS ETF Acc_IE0006FM6MI8_07_01_2026.xlsx"
    )
    print(f"\nDataFrame shape: {holdings_df}")
    print(f"\nDataFrame info:")
    print(holdings_df.to_csv("downloads/holdings.csv", index=False, header=False))
