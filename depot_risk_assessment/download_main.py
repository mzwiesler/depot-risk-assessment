import pathlib
import shutil
from typing import Callable

import requests
from depot_risk_assessment.download_hanetf import download_hanetf_report
from depot_risk_assessment.ticker_config import ticker_config
from depot_risk_assessment.download_amundi import download_amundi_report
from depot_risk_assessment.download_invesco import download_invesco_report
from depot_risk_assessment.etf_handling import TickerConfig
from depot_risk_assessment.xslx_helper import read_xlsx_file_without_styles
import logging

logger = logging.getLogger(__name__)


def download_url_as_csv(config: TickerConfig, directory: str = "downloads") -> bool:
    response = requests.get(config.url)

    # Check if the request was successful
    if response.status_code == 200:
        file_path = pathlib.Path(directory) / f"{config.file_name}.csv"
        with open(file_path, "wb") as file:
            file.write(response.content)
        logger.info("CSV file downloaded successfully.")
    else:
        logger.error(f"Failed to download CSV file. Status code: {response.status_code}")
        return False
    return True


def download_wrapper(download_func: Callable[[str, str], bool]) -> Callable[[TickerConfig, str], bool]:
    def download_and_convert_xlsx(config: TickerConfig, directory: str = "downloads") -> bool:
        """Generic function to download ETF report, read xlsx, and save as CSV."""
        success = download_func(config.url, directory)
        if not success:
            return False

        # Find the downloaded xlsx file in download_directory
        download_dir = pathlib.Path(directory)
        xlsx_files = sorted(
            download_dir.glob(f"{config.file_name}*.xlsx"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        logger.info(f"Found xlsx files: {[f.name for f in xlsx_files]}")

        if not xlsx_files:
            print(f"No xlsx file found in {directory}")
            return False

        xlsx_file = xlsx_files[0]
        df = read_xlsx_file_without_styles(str(xlsx_file))
        target = pathlib.Path(directory) / f"{config.file_name}.csv"
        df.to_csv(target, index=False, header=False)
        print(f"✓ Converted {xlsx_file.name} to {target}")
        return True

    return download_and_convert_xlsx


editor_download_functions: dict[str, Callable[[TickerConfig, str], bool]] = {
    "amundi": download_wrapper(download_amundi_report),
    "invesco": download_wrapper(download_invesco_report),
    "iShares": download_url_as_csv,
    "hanetf": download_wrapper(download_hanetf_report),
    "xtrackers": download_wrapper(download_hanetf_report),
}

# Convert dictionary to TickerConfig objects
ticker_configs = [TickerConfig(wkn=wkn, **config_data) for wkn, config_data in ticker_config.items()]

success = {}
download_directory = "./downloads"

# Clean up and recreate download directory
download_path = pathlib.Path(download_directory)
if download_path.exists():
    logger.info(f"Removing existing directory: {download_directory}")
    shutil.rmtree(download_path)
logger.info(f"Creating directory: {download_directory}")
download_path.mkdir(parents=True, exist_ok=True)

for config in ticker_configs:
    editor = config.editor
    download_func = editor_download_functions[editor]

    success[config.wkn] = download_func(config, download_directory)


for ticker, status in success.items():
    print(f"Download for {ticker} successful: {status}")

errors = []
for ticker, status in success.items():
    if not status:
        errors.append(f"Download failed for {ticker}")
if errors:
    raise Exception(f"Download errors occurred: {', '.join(errors)}")  # TODO: Create download exception

print("All downloads completed successfully.")
