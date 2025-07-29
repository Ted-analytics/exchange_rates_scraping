from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from datetime import datetime
import time
import logging
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def scrape_table_data(driver, table):
    """Helper function to scrape data from a single table"""
    try:
        logger.info("Starting to scrape table data")
        rows = table.find_elements(By.TAG_NAME, "tr")
        logger.info(f"Found {len(rows)} rows in table")
        
        if not rows:
            logger.error("No rows found in table")
            return None, None

        # Extract headers (from first row)
        header_row = rows[0]
        headers = [th.text.strip() for th in header_row.find_elements(By.TAG_NAME, "th")]
        
        # Extract data rows (skip header row)
        data_rows = []
        for row in rows[1:]:  # Skip header row
            try:
                cells = row.find_elements(By.TAG_NAME, "td")
                if cells:
                    row_data = [cell.text.strip() for cell in cells]
                    if len(row_data) >= len(headers):  # Ensure we have enough data
                        data_rows.append(row_data)
            except Exception as e:
                logger.error(f"Error processing row: {str(e)}")
                continue

        logger.info(f"Successfully extracted {len(data_rows)} rows from current page")
        return headers, data_rows

    except Exception as e:
        logger.error(f"Error in scrape_table_data: {str(e)}")
        return None, None

def scrape_exchange_rates():
    driver = None
    try:
        logger.info("Setting up Chrome driver...")
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        
        driver = webdriver.Chrome(options=chrome_options)
        wait = WebDriverWait(driver, 20)
        
        logger.info("Fetching BNR Rwanda website...")
        url = "https://www.bnr.rw/exchangeRate"
        driver.get(url)
        
        # Wait for initial page load and table
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        time.sleep(2)  # Additional wait for stability
        
        # Find all page numbers first
        page_links = driver.find_elements(By.CSS_SELECTOR, "ul.pagination li a")
        page_numbers = []
        for link in page_links:
            try:
                number = int(link.text.strip())
                page_numbers.append(number)
            except (ValueError, TypeError):
                continue
        
        max_page = max(page_numbers) if page_numbers else 1
        logger.info(f"Detected {max_page} pages to process")
        
        all_data_rows = []
        headers = None
        
        # Process each page
        for page in range(1, max_page + 1):
            logger.info(f"Processing page {page} of {max_page}")
            
            if page > 1:
                try:
                    # Find the specific page number link
                    page_xpath = f"//ul[contains(@class, 'pagination')]//a[text()='{page}']"
                    page_link = wait.until(EC.element_to_be_clickable((By.XPATH, page_xpath)))
                    
                    # Scroll into view and click
                    driver.execute_script("arguments[0].scrollIntoView(true);", page_link)
                    time.sleep(1)
                    driver.execute_script("arguments[0].click();", page_link)
                    
                    # Wait for table to update
                    time.sleep(2)
                    wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
                    
                except Exception as e:
                    logger.error(f"Error navigating to page {page}: {str(e)}")
                    continue
            
            # Find and scrape current page's table
            try:
                table = wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
                current_headers, current_data_rows = scrape_table_data(driver, table)
                
                if current_data_rows:
                    if headers is None:
                        headers = current_headers
                    all_data_rows.extend(current_data_rows)
                    logger.info(f"Total rows collected so far: {len(all_data_rows)}")
                else:
                    logger.error(f"No data rows found on page {page}")
                    
            except Exception as e:
                logger.error(f"Error processing table on page {page}: {str(e)}")
                continue

        if not all_data_rows:
            logger.error("No data collected from any page")
            return None, None

        # Create DataFrame
        logger.info(f"Creating DataFrame with {len(all_data_rows)} total rows")
        df = pd.DataFrame(all_data_rows, columns=headers)
        
        # Get update time from most recent date
        try:
            update_time = datetime.strptime(df['Date'].iloc[0], '%m/%d/%Y')
        except Exception as e:
            logger.error(f"Error parsing update time: {str(e)}")
            update_time = datetime.now()
        
        logger.info(f"Final DataFrame shape: {df.shape}")
        logger.info(f"DataFrame columns: {df.columns.tolist()}")
        logger.info(f"First few rows:\n{df.head()}")
        
        return df, update_time
        
    except Exception as e:
        logger.error(f"Error in scrape_exchange_rates: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return None, None
        
    finally:
        if driver:
            driver.quit()

def clean_exchange_rates(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and transform the scraped exchange rates data."""
    try:
        if df is None:
            logger.error("Input DataFrame is None")
            return None
            
        logger.info("Starting data cleaning...")
        logger.info(f"Input DataFrame shape: {df.shape}")
        
        # Make a copy to avoid modifying the original
        cleaned_df = df.copy()
        
        # Standardize column names
        column_mapping = {
            'Currency': 'currency',
            'Buying Rate': 'buying_rate',
            'Average Rate': 'average_rate',
            'Selling Rate': 'selling_rate',
            'Date': 'rate_date'
        }
        
        cleaned_df.rename(columns=column_mapping, inplace=True)
        logger.info("Columns renamed successfully")
        
        # Convert rate columns to numeric, removing any commas
        rate_columns = ['buying_rate', 'average_rate', 'selling_rate']
        for col in rate_columns:
            cleaned_df[col] = pd.to_numeric(cleaned_df[col].str.replace(',', ''), errors='coerce')
            logger.info(f"Converted {col} to numeric")
        
        # Clean currency names
        cleaned_df['currency'] = cleaned_df['currency'].str.strip().str.upper()
        
        # Convert date to datetime
        cleaned_df['rate_date'] = pd.to_datetime(cleaned_df['rate_date'], format='%m/%d/%Y').dt.date
        
        # Drop any rows with missing values
        cleaned_df = cleaned_df.dropna()
        
        logger.info(f"Final cleaned DataFrame shape: {cleaned_df.shape}")
        logger.info(f"Sample of cleaned data:\n{cleaned_df.head()}")
        
        return cleaned_df
        
    except Exception as e:
        logger.error(f"Error cleaning exchange rates data: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return None