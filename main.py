import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse
import time
import re
import os
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
import math
import logging
import pyodbc
import glob
import concurrent.futures
import json
import sqlite3

# ==================== CONFIGURATION ====================
# Configuration class to hold all parameters
class Config:
    # Mode settings
    MODE = "update"  # Options: "full" or "update"
    PAGINATION_MODE = "manual"  # Options: "automatic" or "manual"
    CATEGORY_TYPE_MODE = "automatic"  # Options: "automatic" or "manual"
    
    # Manual settings if modes are set to "manual"
    MANUAL_CATEGORY = "prodaja"  # Options: "prodaja" or "izdavanje"
    MANUAL_TYPE = "stanova"  # Options: e.g., "stanova", "kuca", "hala", etc.
    MANUAL_START_PAGE = 1
    MANUAL_END_PAGE = 5
    CATEGORY_MAPPING = {
        'stanova': 'STANOVI',
        'soba': 'SOBE',
        'kuca': 'KUĆE',
        'garaza': 'GARAŽE',
        'zemljista': 'ZEMLJIŠTE',
        'lokala': 'POSLOVNI PROSTORI',
        'kancelarijskog-prostora': 'POSLOVNI PROSTORI',
        'poslovnih-zgrada': 'POSLOVNI PROSTORI',
        'hala': 'POSLOVNI PROSTORI',
        'magacina': 'POSLOVNI PROSTORI',
        'ugostiteljskih-objekata': 'POSLOVNI PROSTORI',
        'kioska': 'POSLOVNI PROSTORI',
        'stovarista': 'POSLOVNI PROSTORI'
    }
    
    # Website configuration
    WEBSITE = "halooglasi.com"
    BASE_URL = "https://www.halooglasi.com"
    
    # Categories and types to scrape
    CATEGORIES = {
        "prodaja": ["stanova", "soba", "kuca", "garaza", "zemljista", "lokala", 
                    "kancelarijskog-prostora", "poslovnih-zgrada", "hala", "magacina", 
                    "ugostiteljskih-objekata", "kioska", "stovarista"],
        "izdavanje": ["stanova", "soba", "kuca", "garaza", "zemljista", "lokala", 
                    "kancelarijskog-prostora", "poslovnih-zgrada", "hala", "magacina", 
                    "ugostiteljskih-objekata", "kioska", "stovarista"]
    }
    
    # SQL Server configuration
    SQL_SERVER = 'your_server'
    SQL_DATABASE = 'your_database'
    SQL_USERNAME = 'your_username'
    SQL_PASSWORD = 'your_password'
    
    # SQLite fallback configuration
    SQLITE_DB_PATH = 'scraped_data/real_estate_listings.db'
    
    # Concurrent processing
    MAX_THREADS = 5  # Maximum number of concurrent page requests
    DELAY_BETWEEN_REQUESTS = 0.5  # Delay between page requests (seconds)
    
    # Output file configuration
    OUTPUT_DIR = 'scraped_data'
    OUTPUT_PREFIX = f'01.{WEBSITE}_FINAL'
    
    # Cache configuration
    CACHE_DIR = 'cache'
    PAGE_COUNT_CACHE = 'page_count_cache.json'

# Create a config object
config = Config()

# ==================== SETUP ====================
# Create output and cache directories if they don't exist
for directory in [config.OUTPUT_DIR, config.CACHE_DIR]:
    if not os.path.exists(directory):
        os.makedirs(directory)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{config.OUTPUT_DIR}/{config.OUTPUT_PREFIX}_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Record the start time
start_time = time.time()

# Headers to mimic a real browser request
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
}

# ==================== UTILITY FUNCTIONS ====================
def load_page_count_cache():
    '''Load page count cache from file.'''
    cache_file = os.path.join(config.CACHE_DIR, config.PAGE_COUNT_CACHE)
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError:
            logger.warning("Cache file is corrupted, creating new cache")
    return {}

def save_page_count_cache(cache):
    """Save page count cache to file."""
    cache_file = os.path.join(config.CACHE_DIR, config.PAGE_COUNT_CACHE)
    with open(cache_file, 'w') as f:
        json.dump(cache, f)

# Load the page count cache
page_count_cache = load_page_count_cache()

# ==================== WEB SCRAPING FUNCTIONS ====================
def initialize_webdriver():
    '''Initialize and return a Selenium WebDriver with optimized settings.'''
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-notifications")
    options.page_load_strategy = 'eager'  # Don't wait for all resources to load
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.set_page_load_timeout(30)  # Set timeout to 30 seconds
    return driver

def get_last_page(url, driver):
    '''Get the last page number from pagination with caching.'''
    # Check if page count is in cache
    cache_key = url
    if cache_key in page_count_cache:
        logger.info(f"Using cached page count for URL: {url}")
        return page_count_cache[cache_key]
    
    logger.info(f"Getting last page for URL: {url}")
    try:
        driver.get(url)
        time.sleep(1.5)  # Reduced wait time
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        
        # Try to find pagination
        outer_div = soup.find("div", class_="widget-pager widget-pager-ad widget-pager-ad-list-basic")
        if outer_div:
            pagination_div = outer_div.find("div", id="pager-1")
            if pagination_div:
                numbers = [int(li.text) for li in pagination_div.find_all("li") if li.text.strip().isdigit()]
                last_page = max(numbers) if numbers else 1
                
                # Cache the result
                page_count_cache[cache_key] = last_page
                save_page_count_cache(page_count_cache)
                
                logger.info(f"Last page found: {last_page}")
                return last_page
        
        # Default to 1 if pagination not found
        logger.warning(f"No pagination found, returning 1 as last page for URL: {url}")
        page_count_cache[cache_key] = 1
        save_page_count_cache(page_count_cache)
        return 1
    
    except Exception as e:
        logger.error(f"Error getting last page for URL {url}: {str(e)}")
        return 1

def parse_listing(listing, category, type_name):
    '''Parse a single listing to extract data.'''
    try:
        # Extract basic listing data
        title_tag = listing.find("h3", class_="product-title")
        title = title_tag.text.strip() if title_tag else "N/A"
        
        # Skip listings with title "N/A"
        if title.startswith("N/A"):
            return None
        
        listing_url = urljoin(config.BASE_URL, title_tag.a["href"]) if title_tag and title_tag.a else "N/A"
        listing_url = urlunparse(urlparse(listing_url)._replace(query=""))
        listing_id = re.search(r'/(\d+)', listing_url)
        listing_id = listing_id.group(1) if listing_id else "N/A"
        
        # Check if ID is too short (less than 3 digits) or invalid and generate a specific 13-digit ID if needed
        if listing_id == "N/A" or len(listing_id) < 3:
            import random
            # Generate 10 random digits to follow "542"
            random_part = ''.join([str(random.randint(0, 9)) for _ in range(10)])
            listing_id = f"542{random_part}"  # Ensures 13 digits starting with 542
            logger.info(f"Generated 13-digit ID starting with 542: {listing_id} for listing with URL: {listing_url}")

        # Extract price information
        price_tag = listing.find("div", class_="central-feature")
        price = re.sub(r'[^\d]', '', price_tag.text.strip()) if price_tag else "N/A"

        price_per_m2_tag = listing.find("div", class_="price-by-surface")
        price_per_m2 = re.sub(r'[^\d]', '', price_per_m2_tag.text.strip()) if price_per_m2_tag else "N/A"

        # Extract location information
        location_tag = listing.find("ul", class_="subtitle-places")
        location = ", ".join([li.text.strip() for li in location_tag.find_all("li")]) if location_tag else "N/A"
        location_parts = location.split(", ")
        grad = location_parts[0] if len(location_parts) > 0 else "N/A"
        opstina = location_parts[1] if len(location_parts) > 1 else "N/A"
        deo_opstine = location_parts[2] if len(location_parts) > 2 else "N/A"
        ulica = location_parts[3] if len(location_parts) > 3 else "N/A"

        # Extract property features
        features = listing.find_all("li", class_="col-p-1-3")
        size = re.sub(r'[^\d]', '', features[0].text.strip()) if len(features) > 0 else "N/A"
        rooms = re.sub(r'[^\d.]', '', features[1].text.strip()) if len(features) > 1 else "N/A"
        
        floor_info = features[2].text.strip() if len(features) > 2 else "N/A"
        floor_parts = floor_info.split("/")
        
        # Remove unwanted words from floor
        floor = floor_parts[0].strip() if len(floor_parts) > 0 else "N/A"
        for word in ["Spratnost", "PR", "PSUT", "Broj soba", "+"]:
            floor = floor.replace(word, "").strip()
        
        # Remove unwanted words from total_floors
        total_floors = floor_parts[1].strip() if len(floor_parts) > 1 else "N/A"
        total_floors = total_floors.replace("Spratnost", "").strip()

        # Extract description and date
        desc_tag = listing.find("p", class_="product-description")
        description = desc_tag.text.strip() if desc_tag else "N/A"

        date_tag = listing.find("span", class_="publish-date")
        publish_date = date_tag.text.strip() if date_tag else "N/A"

        # Determine advertiser type (placeholder for now)
        advertiser = "N/A"

        # Create listing data dictionary
        new_category = config.CATEGORY_MAPPING.get(type_name, type_name)
        
        listing_data = {
            "Title": title,
            "Location": location,
            "Price": float(price) if price.isdigit() else None,
            "Price per m²": float(price_per_m2) if price_per_m2.isdigit() else None,
            "Size": float(size) if size.isdigit() else None,
            "Rooms": float(rooms) if rooms.replace('.', '').isdigit() else None,
            "Floor": floor,
            "Total Floors": int(total_floors) if total_floors.isdigit() else None,
            "Features": f"Size: {size}, Rooms: {rooms}, Floor: {floor}/{total_floors}",
            "Link": listing_url,
            "ID": int(listing_id) if listing_id.isdigit() else None,
            "Website": config.WEBSITE,
            "Scrapped date": datetime.now().date(),
            "New Category": new_category,
            "Type": type_name.replace("-", " "),
            "Description": description,
            "Publication Date": datetime.strptime(publish_date, '%d.%m.%Y.').date() if publish_date != "N/A" else None,
            "Advertiser": advertiser,
            "City": grad,
            "Municipality": opstina,
            "Part of Municipality": deo_opstine,
            "Ulica": ulica,
            "Country": "Serbia",  # Default value
            "Book status": "N/A",  # Default value
            "Heating": "N/A",  # Default value
            "Views": 0,  # Default value
            "Likes": 0   # Default value
        }
        
        return listing_data
    except Exception as e:
        logger.error(f"Error processing listing: {str(e)}")
        return None

    
def scrape_page(url, category, type_name):
    '''Scrape a single page of listings with better error handling.'''
    logger.info(f"Scraping URL: {url}")
    page_data = []
    
    # Retry mechanism
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code != 200:
                logger.error(f"Failed to retrieve page. Status code: {response.status_code}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying... (Attempt {attempt + 1}/{max_retries})")
                    time.sleep(2)  # Wait before retrying
                    continue
                return page_data
            
            soup = BeautifulSoup(response.content, "html.parser")
            listings = soup.find_all("div", class_="product-item")
            logger.info(f"Found {len(listings)} listings on this page")

            for listing in listings:
                listing_data = parse_listing(listing, category, type_name)
                if listing_data:  # Only add non-None listings
                    page_data.append(listing_data)
            
            # Success, break the retry loop
            break
        
        except (requests.RequestException, Exception) as e:
            logger.error(f"Error scraping page {url}: {str(e)}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(2)  # Wait before retrying
            else:
                logger.error(f"Failed to scrape page after {max_retries} attempts")
    
    return page_data

def get_url_for_category_type(category, type_name):
    '''Generate URL for a specific category and type.'''
    return f"{config.BASE_URL}/nekretnine/{category}-{type_name}"

# ==================== DATA PROCESSING FUNCTIONS ====================
def scrape_pages_concurrently(base_url, start_page, end_page, category, type_name):
    '''Scrape multiple pages concurrently using ThreadPoolExecutor.'''
    all_page_data = []
    urls = [f"{base_url}?page={page}" for page in range(start_page, end_page + 1)]
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=config.MAX_THREADS) as executor:
        # Submit all scraping tasks
        future_to_url = {
            executor.submit(scrape_page, url, category, type_name): url
            for url in urls
        }
        
        # Process as they complete
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                page_data = future.result()
                all_page_data.extend(page_data)
                logger.info(f"Scraped {len(page_data)} listings from {url}")
                
                # Add a short delay between submissions to avoid overwhelming the server
                time.sleep(config.DELAY_BETWEEN_REQUESTS)
            except Exception as e:
                logger.error(f"Error processing results from {url}: {str(e)}")
    
    return all_page_data

def scrape_category_type(category, type_name, driver, data_list):
    '''Scrape all pages for a specific category and type.'''
    base_url = get_url_for_category_type(category, type_name)
    
    # Get total number of pages
    last_page = get_last_page(base_url, driver)
    
    # Determine number of pages to scrape based on mode
    if config.MODE == "full":
        num_pages = last_page
    else:  # update mode
        num_pages = math.ceil(last_page / 20)
    
    logger.info(f"Category: {category}, Type: {type_name}, Total pages: {last_page}, Pages to scrape: {num_pages}")
    
    # Determine page range based on pagination mode
    if config.PAGINATION_MODE == "automatic":
        start_page, end_page = 1, num_pages
    else:  # manual mode
        start_page, end_page = config.MANUAL_START_PAGE, min(config.MANUAL_END_PAGE, num_pages)
    
    # Scrape pages concurrently
    page_data = scrape_pages_concurrently(base_url, start_page, end_page, category, type_name)
    data_list.extend(page_data)
    
    return len(page_data)

# ==================== DATA SAVING FUNCTIONS ====================
def save_data_to_files(data_list):
    '''Save scraped data to CSV and Excel files with better handling of duplicates.'''
    if not data_list:
        logger.warning("No data to save")
        return 0, 0, 0
    
    df = pd.DataFrame(data_list)
    
    # Create filenames with timestamps
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_file = f"{config.OUTPUT_DIR}/{config.OUTPUT_PREFIX}_extracted_data.csv"
    backup_csv_file = f"{config.OUTPUT_DIR}/{config.OUTPUT_PREFIX}_extracted_data_backup_{timestamp}.csv"
    backup_excel_file = f"{config.OUTPUT_DIR}/{config.OUTPUT_PREFIX}_extracted_data_backup_{timestamp}.xlsx"
    excel_file = f"{config.OUTPUT_DIR}/{config.OUTPUT_PREFIX}_extracted_data.xlsx"
    
    # Save backup files
    logger.info(f"Saving backup files: {backup_csv_file} and {backup_excel_file}")
    df.to_csv(backup_csv_file, index=False, encoding='utf-8-sig')
    df.to_excel(backup_excel_file, index=False, engine='openpyxl')
    
    # Check if main CSV exists and load it
    if os.path.isfile(csv_file):
        try:
            logger.info(f"Loading existing CSV: {csv_file}")
            df_existing = pd.read_csv(csv_file, encoding='utf-8-sig')
            # Concatenate with new data
            df_full = pd.concat([df_existing, df], ignore_index=True)
        except Exception as e:
            logger.error(f"Error loading existing CSV, creating new file: {str(e)}")
            df_full = df
    else:
        logger.info(f"Creating new CSV: {csv_file}")
        df_full = df
    
    # Save the combined data
    df_full.to_csv(csv_file, index=False, encoding='utf-8-sig')
    
    # Remove duplicates
    initial_rows = len(df_full)
    logger.info(f"Initial row count: {initial_rows}")
    
    df_cleaned = df_full.drop_duplicates(subset=['ID', 'Link'])
    final_rows = len(df_cleaned)
    logger.info(f"Final row count after removing duplicates: {final_rows}")
    
    removed_rows = initial_rows - final_rows
    logger.info(f"Removed {removed_rows} duplicate rows")
    
    # Save cleaned data to Excel
    df_cleaned.to_excel(excel_file, index=False, engine='openpyxl')
    logger.info(f"Saved cleaned data to Excel: {excel_file}")
    
    return initial_rows, final_rows, removed_rows

def merge_excel_files():
    '''Merge all Excel files into a single file with optimization for large files.'''
    excel_files = glob.glob(f"{config.OUTPUT_DIR}/*_FINAL_extracted_data.xlsx")
    
    if not excel_files:
        logger.warning("No Excel files found to merge.")
        return
    
    logger.info(f"Found {len(excel_files)} Excel files to merge")
    
    # Initialize an empty DataFrame to store the merged data
    merged_df = pd.DataFrame()
    total_rows = 0
    
    # Read and merge each Excel file
    for file in excel_files:
        logger.info(f"Reading file: {file}")
        
        # Read the Excel file in one go (Excel files typically aren't as large as CSV files)
        df = pd.read_excel(file)
        if merged_df.empty:
            merged_df = df
        else:
            merged_df = pd.concat([merged_df, df], ignore_index=True)
        total_rows += len(df)
    
    logger.info(f"Total rows before removing duplicates: {total_rows}")
    
    # Remove duplicates
    merged_df = merged_df.drop_duplicates(subset=['ID', 'Link'])
    logger.info(f"Total rows after removing duplicates: {len(merged_df)}")
    
    # Save the merged DataFrame
    merged_file = f"{config.OUTPUT_DIR}/merged_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    merged_df.to_excel(merged_file, index=False, engine='openpyxl')
    logger.info(f"Merged data saved to: {merged_file}")

# New function to create SQLite database if SQL Server is not available
def create_sqlite_database():
    try:
        conn = sqlite3.connect(config.SQLITE_DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS RealEstateListings (
            ID INTEGER PRIMARY KEY,
            Title VARCHAR(255),
            Location VARCHAR(255),
            Price DECIMAL(15,2),
            PricePerM2 DECIMAL(10,2),
            Size DECIMAL(10,2),
            Rooms DECIMAL(5,2),
            Floor VARCHAR(255),
            TotalFloors INTEGER,
            Features TEXT,
            Link VARCHAR(500) UNIQUE,
            Website VARCHAR(255),
            ScrappedDate DATE,
            NewCategory VARCHAR(200),
            Type VARCHAR(100),
            Description TEXT,
            PublicationDate DATE,
            Advertiser VARCHAR(255),
            City VARCHAR(100),
            Municipality VARCHAR(100),
            PartOfMunicipality VARCHAR(100),
            Ulica VARCHAR(255),
            Country VARCHAR(100),
            BookStatus VARCHAR(100),
            Heating VARCHAR(100),
            Views INTEGER,
            Likes INTEGER
        )
        ''')
        
        conn.commit()
        return conn, cursor
    except Exception as e:
        logger.error(f"Error creating SQLite database: {str(e)}")
        return None, None

def insert_to_sqlite(conn, cursor, data_df):
    '''Insert data into SQLite database with improved column mapping.'''
    try:
        # Column mapping dictionary to handle different column names
        column_mapping = {
            'Price per m²': 'Price_per_m2',
            'Total Floors': 'Total_Floors',
            'Sajt': 'Website',
            'Datum ekstrakcije': 'Scrapped_date',
            'Publication Date': 'Publication_Date',
            'Grad': 'City',
            'Opština': 'Municipality',
            'Deo opštine': 'Part_of_Municipality'
        }

        # Rename columns according to mapping
        df_cleaned = data_df.copy()
        for old_name, new_name in column_mapping.items():
            if old_name in df_cleaned.columns:
                df_cleaned = df_cleaned.rename(columns={old_name: new_name})

        rows_inserted = 0
        
        # Insert each row into the database
        for index, row in df_cleaned.iterrows():
            sql = '''
            INSERT OR REPLACE INTO RealEstateListings (
                Title, Location, Price, Price_per_m2, Size, Rooms, Floor, Total_Floors,
                Features, Link, ID, Website, Scrapped_date, New_Category, Type,
                Description, Publication_Date, Advertiser, City, Municipality, 
                Part_of_Municipality, Ulica, Country, Book_status, Heating, Views, Likes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            
            # Prepare values with proper null handling
            values = (
                str(row.get('Title', 'N/A')),
                str(row.get('Location', 'N/A')),
                float(row.get('Price', 0)) if pd.notnull(row.get('Price')) else None,
                float(row.get('Price_per_m2', 0)) if pd.notnull(row.get('Price_per_m2')) else None,
                float(row.get('Size', 0)) if pd.notnull(row.get('Size')) else None,
                float(row.get('Rooms', 0)) if pd.notnull(row.get('Rooms')) else None,
                str(row.get('Floor', 'N/A')),
                int(row.get('Total_Floors', 0)) if pd.notnull(row.get('Total_Floors')) else None,
                str(row.get('Features', 'N/A')),
                str(row.get('Link', 'N/A')),
                int(row.get('ID', 0)) if pd.notnull(row.get('ID')) else None,
                str(row.get('Website', 'N/A')),
                str(row.get('Scrapped_date', 'N/A')),
                str(row.get('New_Category', 'N/A')),
                str(row.get('Type', 'N/A')),
                str(row.get('Description', 'N/A')),
                str(row.get('Publication_Date', 'N/A')),
                str(row.get('Advertiser', 'N/A')),
                str(row.get('City', 'N/A')),
                str(row.get('Municipality', 'N/A')),
                str(row.get('Part_of_Municipality', 'N/A')),
                str(row.get('Ulica', 'N/A')),
                str(row.get('Country', 'Serbia')),
                str(row.get('Book_status', 'N/A')),
                str(row.get('Heating', 'N/A')),
                int(row.get('Views', 0)),
                int(row.get('Likes', 0))
            )
            
            try:
                cursor.execute(sql, values)
                rows_inserted += 1
                
                # Commit every 100 rows
                if rows_inserted % 100 == 0:
                    conn.commit()
                    logger.info(f"Inserted {rows_inserted} rows so far to SQLite")
            
            except sqlite3.IntegrityError as e:
                logger.warning(f"Duplicate entry at row {index}, skipping: {str(e)}")
            except Exception as e:
                logger.error(f"Error inserting row {index} into SQLite: {str(e)}")
        
        # Final commit
        conn.commit()
        logger.info(f"Successfully inserted {rows_inserted} rows into SQLite database")
        
        return rows_inserted
    
    except Exception as e:
        logger.error(f"Error in insert_to_sqlite function: {str(e)}")
        return 0

def insert_to_sql(excel_file=None):
    '''Insert data into SQL Server with fallback to SQLite if SQL Server is not available.'''
    try:
        # Use specified Excel file or default
        if not excel_file:
            excel_file = f"{config.OUTPUT_DIR}/{config.OUTPUT_PREFIX}_extracted_data.xlsx"
        
        # Check if file exists
        if not os.path.exists(excel_file):
            logger.error(f"Excel file not found: {excel_file}")
            return
        
        # Read the Excel file
        df = pd.read_excel(excel_file)
        logger.info(f"Read {len(df)} rows from Excel file: {excel_file}")
        
        # Try connecting to SQL Server
        try:
            # Create connection string
            conn_str = f'DRIVER={{SQL Server}};SERVER={config.SQL_SERVER};DATABASE={config.SQL_DATABASE};UID={config.SQL_USERNAME};PWD={config.SQL_PASSWORD}'
            
            # Connect to SQL Server
            logger.info("Connecting to SQL Server")
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            
            # Process data in chunks
            chunk_size = 1000
            rows_inserted = 0
            
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i+chunk_size]
                
                # Insert data in batches
                for index, row in chunk.iterrows():
                    # Create SQL insert statement
                    sql = '''
                    INSERT INTO RealEstateListings (
                        Title, Location, Price, PricePerM2, Size, Rooms, Floor, TotalFloors,
                        Features, Link, ID, Sajt, DatumEkstrakcije, Category, Type,
                        Description, PublicationDate, Advertiser, Grad, Opstina, DeoOpstine, Ulica
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    '''
                    
                    # Convert row to a tuple of values
                    values = (
                        str(row['Title']), str(row['Location']), str(row['Price']), str(row['Price per m²']),
                        str(row['Size']), str(row['Rooms']), str(row['Floor']), str(row['Total Floors']),
                        str(row['Features']), str(row['Link']), str(row['ID']), str(row['Sajt']),
                        str(row['Datum ekstrakcije']), str(row['Category']), str(row['Type']),
                        str(row['Description']), str(row['Publication Date']), str(row['Advertiser']),
                        str(row['Grad']), str(row['Opština']), str(row['Deo opštine']), str(row['Ulica'])
                    )
                    
                    try:
                        cursor.execute(sql, values)
                        rows_inserted += 1
                        
                        # Commit every 100 rows to avoid large transactions
                        if rows_inserted % 100 == 0:
                            conn.commit()
                            logger.info(f"Inserted {rows_inserted} rows to SQL Server so far")
                    
                    except Exception as e:
                        logger.error(f"Error inserting row {index} into SQL Server: {str(e)}")
                
                # Commit at the end of each chunk
                conn.commit()
            
            logger.info(f"Successfully inserted {rows_inserted} rows into SQL Server")
            
            # Close the connection
            cursor.close()
            conn.close()
        
        except (pyodbc.Error, Exception) as e:
            logger.warning(f"Failed to connect to SQL Server: {str(e)}")
            logger.info("Falling back to SQLite database")
            
            # Create SQLite database and insert data
            conn, cursor = create_sqlite_database()
            if conn and cursor:
                rows_inserted = insert_to_sqlite(conn, cursor, df)
                
                # Close the connection
                cursor.close()
                conn.close()
            else:
                logger.error("Failed to create SQLite database")
    
    except Exception as e:
        logger.error(f"Error in insert_to_sql function: {str(e)}")

# ==================== STATISTICS AND LOGGING ====================
def log_statistics(initial_rows, final_rows, removed_rows, data_list):
    '''Log scraping statistics.'''
    end_time = time.time()
    duration = end_time - start_time
    formatted_duration = time.strftime("%H:%M:%S", time.gmtime(duration))
    
    logger.info("=" * 50)
    logger.info("SCRAPING STATISTICS")
    logger.info("=" * 50)
    logger.info(f"Mode: {config.MODE}")
    logger.info(f"Pagination Mode: {config.PAGINATION_MODE}")
    logger.info(f"Category-Type Mode: {config.CATEGORY_TYPE_MODE}")
    logger.info(f"Total records scraped: {len(data_list)}")
    logger.info(f"Initial rows (including previous data): {initial_rows}")
    logger.info(f"Final rows (after removing duplicates): {final_rows}")
    logger.info(f"Rows removed (duplicates): {removed_rows}")
    logger.info(f"Duration: {formatted_duration}")
    logger.info("=" * 50)
    
    print("\nSCRAPING STATISTICS")
    print("=" * 50)
    print(f"Mode: {config.MODE}")
    print(f"Total records scraped: {len(data_list)}")
    print(f"Initial rows: {initial_rows}")
    print(f"Final rows: {final_rows}")
    print(f"Rows removed: {removed_rows}")
    print(f"Duration: {formatted_duration}")
    print("=" * 50)

# ==================== MAIN FUNCTION ====================
def main():
    '''Main function to orchestrate the scraping process.'''
    # Initialize data list
    data_list = []
    
    # Initialize WebDriver
    driver = initialize_webdriver()
    
    try:
        # Validate configuration
        if config.MODE not in ["full", "update"]:
            raise ValueError("Invalid MODE. Please set to 'full' or 'update'.")
        if config.PAGINATION_MODE not in ["automatic", "manual"]:
            raise ValueError("Invalid PAGINATION_MODE. Please set to 'automatic' or 'manual'.")
        if config.CATEGORY_TYPE_MODE not in ["automatic", "manual"]:
            raise ValueError("Invalid CATEGORY_TYPE_MODE. Please set to 'automatic' or 'manual'.")
        
        # Determine categories and types to scrape based on mode
        if config.CATEGORY_TYPE_MODE == "automatic":
            categories_to_scrape = config.CATEGORIES
        else:  # manual mode
            categories_to_scrape = {config.MANUAL_CATEGORY: [config.MANUAL_TYPE]}
        
        # Loop through categories and types
        for category, types in categories_to_scrape.items():
            for type_name in types:
                scrape_category_type(category, type_name, driver, data_list)
        
        # Save data to files
        initial_rows, final_rows, removed_rows = save_data_to_files(data_list)
        
        # Insert data to SQL Server (with fallback to SQLite)
        insert_to_sql()
        
        # Log statistics
        log_statistics(initial_rows, final_rows, removed_rows, data_list)
        
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
    
    finally:
        # Clean up resources
        driver.quit()
        logger.info("Scraping process completed.")

if __name__ == "__main__":
    main()



