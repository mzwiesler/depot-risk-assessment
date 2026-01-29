from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from pathlib import Path


def download_amundi_report(url: str, download_directory: str | None = None) -> bool:
    """
    Downloads the ETF components report from an Amundi ETF product page.

    Args:
        url: The Amundi ETF product page URL
        download_directory: Optional path to download directory. If None, uses default downloads folder.

    Returns:
        bool: True if download was successful, False otherwise
    """
    options = Options()
    options.add_argument("--start-maximized")

    if download_directory:
        download_path = str(Path(download_directory).absolute())
        options.add_experimental_option(
            "prefs",
            {
                "download.default_directory": download_path,
                "download.prompt_for_download": False,
            },
        )

    driver = webdriver.Chrome(options=options)

    try:
        driver.get(url)
        wait = WebDriverWait(driver, 10)
        wait_short = WebDriverWait(driver, 4)
        # --- 1. Close popup disclaimer ---
        try:
            popup_priv = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Privatanleger')]")))
            popup_priv.click()
            print("Closed popup disclaimer.")
        except Exception:
            print("Popup disclaimer not shown.")

        # --- 2. Close sticky header disclaimer ---
        try:
            sticky_priv = wait_short.until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//span[@id='disclaimerCountryRedirectionText']/following-sibling::button")
                )
            )
            sticky_priv.click()
            print("Closed sticky header disclaimer.")
        except Exception:
            print("Sticky header disclaimer not shown.")

        # --- 3. Accept cookies ("Akzeptieren und fortfahren") ---
        try:
            accept_cookies = wait_short.until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Akzeptieren und fortfahren')]"))
            )
            accept_cookies.click()
            print("Cookie banner accepted.")
        except Exception as e:
            print("Cookie banner not shown or not clickable:", e)

        # --- 4. Click on Akzeptieren (additional cookie consent) ---
        try:
            wait_short = WebDriverWait(driver, 2)
            akzeptieren_button = wait_short.until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Alle annehmen')]"))
            )
            akzeptieren_button.click()
            print("Clicked Akzeptieren.")
        except Exception as e:
            print("Akzeptieren button not found or not clickable:", e)

        # --- 5. Click on Komponenten download button ---
        try:
            komponenten_button = wait.until(
                EC.presence_of_element_located((By.XPATH, "//a[contains(., 'KOMPONENTEN DES ETFS HERUNTERLADEN')]"))
            )
            # Scroll element into view
            driver.execute_script(
                "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", komponenten_button
            )
            time.sleep(0.5)
            # Click using JavaScript
            driver.execute_script("arguments[0].click();", komponenten_button)
            print("Clicked Komponenten download button.")

            # Wait for download to start
            time.sleep(3)
            return True

        except Exception as e:
            print(f"Komponenten button not found or not clickable: {e}")
            return False

    except Exception as e:
        print(f"Error during download: {e}")
        return False

    finally:
        time.sleep(2)
        driver.quit()


# Example usage
if __name__ == "__main__":
    success = {}
    download_directory = "./downloads"  # Optional: specify download directory
    url_health = "https://www.amundietf.de/de/privatanleger/products/equity/amundi-sp-world-health-care-screened-ucits-etf-acc/ie0006fm6mi8"
    success["health"] = download_amundi_report(url_health, download_directory)
    url_tecdax = "https://www.amundietf.de/de/privatanleger/products/equity/amundi-tecdax-ucits-etf-dist/de000etf9082"
    success["tecdax"] = download_amundi_report(url_tecdax, download_directory)
    url_msci_eur = "https://www.amundietf.de/de/professionell/products/equity/amundi-msci-europe-sri-climate-paris-aligned-ucits-etf-dr-c/lu1861137484#at_medium=Sponsored%20links&at_campaign=Climate_DE&at_platform=google"
    success["msci_eur"] = download_amundi_report(url_msci_eur, download_directory)
    for key, success in success.items():
        print(f"Download for {key} successful: {success}")
