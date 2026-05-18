from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from pathlib import Path
import time


def download_hanetf_report(url: str, download_directory: str | None = None) -> bool:
    """
    Downloads the ETF holdings report from a HANetf holdings page.

    Args:
        url: The HANetf holdings page URL (e.g., https://etp.hanetf.com/INQQ-holdings)
        download_directory: Optional path to download directory. If None, uses default downloads folder.

    Returns:
        bool: True if download was successful, False otherwise
    """
    options = Options()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    if download_directory:
        download_path = str(Path(download_directory).absolute())
        options.add_experimental_option(
            "prefs",
            {
                "download.default_directory": download_path,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
            },
        )
    else:
        download_path = None

    driver = webdriver.Chrome(options=options)

    try:
        print(f"Navigating to {url}...")
        driver.get(url)

        # Wait for page to load
        print("Waiting for page to load and download to start...")
        time.sleep(3)

        return True

    except Exception as e:
        print(f"✗ Error during download: {e}")
        return False

    finally:
        driver.quit()


# Example usage
if __name__ == "__main__":
    url = "https://etp.hanetf.com/INQQ-holdings"
    download_directory = "./downloads"
    success = download_hanetf_report(url, download_directory)
    if success:
        print("Download completed successfully!")
    else:
        print("Download failed.")
