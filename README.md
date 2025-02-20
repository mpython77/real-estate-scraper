# Real Estate Scraper

This project is designed to scrape real estate listings from **halooglasi.com**. The scraper utilizes Selenium and BeautifulSoup to scan web pages and extract data.

## Features
- Parse web pages using Selenium and BeautifulSoup
- Use JSON cache to speed up page count retrieval
- Store data in SQL Server or SQLite
- Save data in CSV and Excel formats
- Multi-threaded processing for faster execution

## Installation
To run this project, you need Python 3.x installed.

1. **Clone the repository:**
   ```bash
   git clone https://github.com/mpython77/real-estate-scraper.git
   cd real-estate-scraper
   ```
2. **Install required dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
3. **SQL Server configuration (optional):**
   If using SQL Server, update the following parameters in the `Config` class:
   ```python
   SQL_SERVER = 'your_server'
   SQL_DATABASE = 'your_database'
   SQL_USERNAME = 'your_username'
   SQL_PASSWORD = 'your_password'
   ```

## Running the Script

```bash
python Project.py
```

## Dependencies
- requests
- beautifulsoup4
- selenium
- pandas
- sqlite3
- json
- pyodbc
- webdriver-manager
- concurrent.futures
- openpyxl
- logging

## Data Storage
The scraped data is stored in the following formats:
- CSV: `scraped_data/01.halooglasi_FINAL_extracted_data.csv`
- Excel: `scraped_data/01.halooglasi_FINAL_extracted_data.xlsx`
- SQLite: `scraped_data/real_estate_listings.db`

## .gitignore
```
__pycache__/
venv/
*.db
*.log
scraped_data/
cache/
```

## Uploading to GitHub
1. **Create a new repository**
   Go to GitHub and create a new repository: [GitHub](https://github.com/)

2. **Link the repository to your local project:**
   ```bash
   cd C:\Users\kelaj\OneDrive\Ishchi stol\real-estate-scraper
   git init
   git remote add origin https://github.com/mpython77/real-estate-scraper.git
   ```

3. **Add and commit files:**
   ```bash
   git add README.md requirements.txt LICENSE Project.py .gitignore
   git commit -m "Initial commit"
   ```

4. **Push to GitHub:**
   ```bash
   git branch -M main
   git push -u origin main
   ```

## License
Distributed under the MIT License.

