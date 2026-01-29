from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from pathlib import Path


def download_invesco_report(url: str, download_directory: str | None = None) -> bool:
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
        # Step 1: Navigate directly to the product page
        print("Step 1: Navigating to product page...")
        driver.get(url)
        wait = WebDriverWait(driver, 8)
        wait_short = WebDriverWait(driver, 4)
        time.sleep(2)

        # --- 1. Handle cookies first - reject or accept ---
        try:
            # First try to reject all cookies (appears before investor type modal)
            reject_cookies = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Alle cookies ablehnen')]"))
            )
            reject_cookies.click()
            print("✓ Clicked 'Alle cookies ablehnen'.")
            time.sleep(2)
        except Exception:
            print("Cookie dialog not shown.")

        # --- 2. Handle investor type selection modal (appears after cookies) ---
        try:
            print("Step 2: Looking for investor type selection modal...")
            # Check if the country-splash modal is present
            wait_short.until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'country-splash')]")))
            print("✓ Found country-splash modal")

            # Try to find and click the Privatanleger button
            try:
                privatanleger_button = wait.until(
                    EC.element_to_be_clickable((By.XPATH, "//div[@data-audience='Privatanleger']//button"))
                )
                privatanleger_button.click()
                print("✓ Clicked 'Privatanleger' button.")
                time.sleep(1)  # Wait for modal to close and page to load
                bestatigen_button = wait.until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Bestätigen')]"))
                )
                bestatigen_button.click()
                print("✓ Clicked 'Bestätigen' button.")
                time.sleep(1)  # Wait for page to load
            except Exception:
                # Fallback: ask user to do it manually
                print(">>> Could not auto-click. Please select 'Privatanleger' in the browser window...")
                input(">>> Press Enter after you have selected the investor type and the modal has closed...")
                print("✓ User confirmed. Continuing...")
                time.sleep(1)
        except Exception:
            print("✓ Privatanleger modal not shown (likely already selected).")

        # --- 3. Click on Daten extrahieren ---
        try:
            # Try multiple selectors
            daten_button = None
            selectors = [
                "//button[contains(., 'Daten exportieren')]",
            ]
            for selector in selectors:
                try:
                    daten_button = driver.find_element(By.XPATH, selector)
                    if daten_button:
                        print(f"Found 'Daten extrahieren' with selector: {selector}")
                        break
                except Exception:
                    continue

            if daten_button:
                # Scroll element into view
                driver.execute_script(
                    "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});",
                    daten_button,
                )
                time.sleep(0.5)
                # Click using JavaScript
                driver.execute_script("arguments[0].click();", daten_button)
                print("Clicked 'Daten extrahieren'.")

                # Wait for download to start
                time.sleep(5)
                return True
            else:
                print("'Daten extrahieren' button not found with any selector.")
                return False

        except Exception as e:
            print(f"'Daten extrahieren' button not found or not clickable: {e}")
            return False

    except Exception as e:
        print(f"Error during download: {e}")
        return False

    finally:
        time.sleep(2)
        driver.quit()


# Example usage
if __name__ == "__main__":
    url_nasdaq = (
        "https://www.invesco.com/de/de/financial-products/etfs/invesco-eqqq-nasdaq-100-ucits-etf-acc.html#Positionen"
    )
    download_directory = "./downloads"  # Optional: specify download directory
    success = download_invesco_report(url_nasdaq, download_directory)
    if success:
        print("Download completed successfully!")
    else:
        print("Download failed.")
