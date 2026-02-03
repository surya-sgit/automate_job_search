"""
Job Search Automation Agent
---------------------------
Automates the process of finding jobs, filtering them based on experience,
and saving them to a Google Sheet with professional formatting.

Features:
- AI-driven search query generation (Gemini)
- Multi-site scraping (LinkedIn, Indeed)
- Experience filtering (Removes Senior/Lead roles)
- Robust connection handling (Retries on network failure)
- Professional Sheet formatting (Bolding, Freezing, Checkboxes)

Author: Surya Prakash Baid
Date: 2026-02-03
"""

import os
import time
import logging
from typing import List, Optional

import pandas as pd
import PyPDF2
import gspread
from dotenv import load_dotenv
from google import genai
from oauth2client.service_account import ServiceAccountCredentials
from jobspy import scrape_jobs

# --- Optional Formatting Dependencies ---
try:
    from gspread_formatting import format_cell_range, cellFormat, textFormat
    from gspread import DataValidationRule, BooleanCondition
    FORMATTING_AVAILABLE = True
except ImportError:
    FORMATTING_AVAILABLE = False

# --- Configuration ---
load_dotenv()
CONFIG = {
    "GEMINI_KEY": os.getenv("GEMINI_API_KEY"),
    "CREDS_FILE": "credentials.json",
    "RESUME_FILE": "Surya_Prakash_Baid.pdf",
    "SHEET_NAME": "Daily_Job_Hunt",
    "SENIOR_KEYWORDS": [
        r'\bsenior\b', r'\blead\b', r'\bmanager\b', r'\bprincipal\b',
        r'\barchitect\b', r'\bhead\b', r'\bdirector\b', r'\bvp\b',
        r'5\+\s*years', r'6\+\s*years'
    ]
}

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def _get_sheet_client() -> Optional[gspread.Worksheet]:
    """
    Helper function to authenticate and retrieve the Google Sheet.
    Includes a Retry Mechanism for stability against network blips.
    """
    max_retries = 3
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    for attempt in range(max_retries):
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name(CONFIG["CREDS_FILE"], scope)
            client = gspread.authorize(creds)
            
            try:
                return client.open(CONFIG["SHEET_NAME"]).sheet1
            except gspread.exceptions.SpreadsheetNotFound:
                logger.info(f"Sheet '{CONFIG['SHEET_NAME']}' not found. Creating new sheet.")
                sheet = client.create(CONFIG["SHEET_NAME"]).sheet1
                client.insert_permission(sheet.spreadsheet.id, creds.service_account_email, perm_type='user', role='owner')
                return sheet
                
        except Exception as e:
            logger.warning(f"Connection attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(5)  # Wait 5 seconds before trying again
            else:
                logger.critical(f"Google Sheets connection failed after {max_retries} attempts.")
                return None

def get_search_queries() -> List[str]:
    """
    Analyzes the user's resume using Gemini AI to generate relevant search queries.

    Returns:
        List[str]: A list of search queries in 'Role | Location' format.
    """
    logger.info("Reading resume file...")
    try:
        reader = PyPDF2.PdfReader(CONFIG["RESUME_FILE"])
        text = "".join([page.extract_text() for page in reader.pages])

        client = genai.Client(api_key=CONFIG["GEMINI_KEY"])
        
        prompt = f"""
        Analyze this resume and generate a python list of 5 search queries for LinkedIn/Indeed.
        Focus on skills: Generative AI, Data Science, Python, Computer Vision, Deep Learning.
        Location: India (Remote or On-site).
        Format: ["Role | Location", "Role | Location"]
        Resume context: {text[:3000]}
        """
        
        response = client.models.generate_content(
            model='gemini-2.0-flash',
            contents=prompt
        )
        
        # Clean and parse response
        clean_text = response.text.replace("```python", "").replace("```", "").strip()
        return eval(clean_text)

    except Exception as e:
        logger.error(f"AI Generation failed: {e}. Reverting to fallback queries.")
        return [
            "Generative AI Engineer | India",
            "Data Scientist | India",
            "Python Developer | India",
            "Computer Vision Engineer | India",
            "Software Engineer Fresher | India"
        ]

def filter_experience(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters out job roles that require significant experience based on title keywords.

    Args:
        df (pd.DataFrame): The raw dataframe of scraped jobs.

    Returns:
        pd.DataFrame: The filtered dataframe suitable for a fresher/junior.
    """
    if df.empty:
        return df

    initial_count = len(df)
    pattern = '|'.join(CONFIG["SENIOR_KEYWORDS"])
    
    # Filter rows where title does NOT match the senior pattern
    filtered_df = df[~df['title'].str.contains(pattern, case=False, na=False, regex=True)]
    
    removed_count = initial_count - len(filtered_df)
    logger.info(f"Filtered {removed_count} senior-level roles.")
    
    return filtered_df

def run_scraper(queries: List[str]) -> pd.DataFrame:
    """
    Executes the job scraper for each query in the list.

    Args:
        queries (List[str]): List of search terms.

    Returns:
        pd.DataFrame: A combined dataframe of all found jobs.
    """
    all_jobs = []
    
    for query in queries:
        try:
            role, loc = query.split("|")
            logger.info(f"Scraping: {role.strip()} in {loc.strip()}")
            
            jobs = scrape_jobs(
                site_name=["linkedin", "indeed"],
                search_term=role.strip(),
                location=loc.strip(),
                results_wanted=5,
                hours_old=72,
                country_indeed='India'
            )
            
            # Normalize Link Column
            if 'job_url' in jobs.columns:
                jobs['apply_link'] = jobs['job_url_direct'].fillna(jobs['job_url'])
            else:
                jobs['apply_link'] = jobs['job_url_direct']
                
            all_jobs.append(jobs)
            time.sleep(2)  # Respectful delay
            
        except Exception as e:
            logger.warning(f"Failed to scrape '{query}': {e}")

    if not all_jobs:
        return pd.DataFrame()

    combined_df = pd.concat(all_jobs, ignore_index=True)
    return filter_experience(combined_df)

def save_to_sheet(df: pd.DataFrame):
    """
    Saves new jobs to Google Sheets with duplicate checking and formatting.
    """
    if df.empty:
        logger.info("No jobs found to save.")
        return

    sheet = _get_sheet_client()
    if not sheet:
        return

    # Prepare Data
    cols = ['site', 'title', 'company', 'location', 'date_posted', 'apply_link']
    available_cols = [c for c in cols if c in df.columns]
    df = df[available_cols].astype(str)

    # Duplicate Check
    try:
        existing_data = sheet.get_all_values()
        if len(existing_data) > 1:
            # Assumes 'apply_link' is at index 5
            existing_links = set(row[5] for row in existing_data[1:] if len(row) > 5)
            df = df[~df['apply_link'].isin(existing_links)]
            
            if df.empty:
                logger.info("All jobs already exist in the sheet.")
                return
        else:
            # Initialize Sheet with Headers
            header = cols + ['Applied?']
            sheet.append_row(header)
            if FORMATTING_AVAILABLE:
                fmt = cellFormat(textFormat=textFormat(bold=True))
                format_cell_range(sheet, 'A1:G1', fmt)
                sheet.freeze(rows=1)

    except Exception as e:
        logger.error(f"Error reading existing data: {e}")
        return

    # Append New Data
    df['Applied?'] = "FALSE"
    try:
        sheet.append_rows(df.values.tolist())
        logger.info(f"Successfully appended {len(df)} new jobs.")

        # Apply Checkboxes (if library available)
        if FORMATTING_AVAILABLE:
            total_rows = len(sheet.get_all_values())
            new_rows = len(df)
            start_row = total_rows - new_rows + 1
            
            rule = DataValidationRule(
                BooleanCondition('BOOLEAN'),
                showCustomUi=True
            )
            sheet.set_data_validation(f"G{start_row}:G{total_rows}", rule)
            logger.info("Checkbox validation applied.")

    except Exception as e:
        logger.error(f"Failed to append data: {e}")

if __name__ == "__main__":
    logger.info("Starting Job Search Agent...")
    
    search_queries = get_search_queries()
    logger.info(f"Generated Queries: {search_queries}")
    
    job_results = run_scraper(search_queries)
    save_to_sheet(job_results)
    
    logger.info("Process completed.")