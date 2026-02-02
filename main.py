import os
import time
import PyPDF2
import pandas as pd
import gspread
from dotenv import load_dotenv
from google import genai 
from oauth2client.service_account import ServiceAccountCredentials
from jobspy import scrape_jobs

# 1. Load Environment Variables
load_dotenv()
GEMINI_KEY = os.getenv("GEMINI_API_KEY") 
GOOGLE_CREDS_FILE = "credentials.json" 
RESUME_FILE = "Surya_Prakash_Baid.pdf" 

def get_search_queries():
    """Agent 1: Reads Resume and decides search queries."""
    print("Reading Resume...")
    try:
        # Try to read resume
        reader = PyPDF2.PdfReader(RESUME_FILE)
        text = "".join([page.extract_text() for page in reader.pages])
        
        client = genai.Client(api_key=GEMINI_KEY)
        
        prompt = f"""
        Analyze this resume and generate a python list of 5 search queries for LinkedIn/Indeed.
        Focus on these skills: Generative AI, Data Science, Python, Computer Vision, Deep Learning.
        Location: India (Remote or On-site).
        Format: ["Role | Location", "Role | Location"]
        Resume: {text[:3000]}
        """
        
        response = client.models.generate_content(
            model='gemini-2.0-flash',
            contents=prompt
        )
        
        clean_text = response.text.replace("```python", "").replace("```", "").strip()
        queries = eval(clean_text)
        return queries

    except Exception as e:
        print(f"AI Error: {e}")
        print("Switching to Manual 'Pan-India' Skill Search...")
        # FALLBACK: Matches your Resume Skills + Whole India
        return [
            "Generative AI Engineer | India",
            "Data Scientist | India",
            "Python Developer | India",
            "Computer Vision Engineer | India",
            "Software Engineer Fresher | India"
        ]

def run_scraper(queries):
    """Agent 2: Scrapes Jobs using JobSpy."""
    all_jobs = []
    for q in queries:
        role, loc = q.split("|")
        print(f"Searching: {role.strip()} in {loc.strip()}...")
        try:
            jobs = scrape_jobs(
                site_name=["linkedin", "indeed"],
                search_term=role.strip(),
                location=loc.strip(),
                results_wanted=5, 
                hours_old=72,
                country_indeed='India'
            )
            
            # --- FIX: Ensure we have a valid link ---
            # If 'job_url_direct' is missing/nan, fill it with 'job_url' (The LinkedIn Page)
            if 'job_url' in jobs.columns:
                jobs['apply_link'] = jobs['job_url_direct'].fillna(jobs['job_url'])
            else:
                jobs['apply_link'] = jobs['job_url_direct']
            
            all_jobs.append(jobs)
            time.sleep(2) 
        except Exception as e:
            print(f"Error on {role}: {e}")
            
    if all_jobs:
        return pd.concat(all_jobs, ignore_index=True)
    return pd.DataFrame()

def save_to_sheet(df):
    """Agent 3: Saves results to Google Sheets."""
    if df.empty:
        print("No jobs found today.")
        return

    print("Saving to Google Sheets...")
    
    # Convert to string to fix JSON errors
    df = df.astype(str)
    
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS_FILE, scope)
    client = gspread.authorize(creds)
    
    try:
        sheet = client.open("Daily_Job_Hunt").sheet1
    except gspread.exceptions.SpreadsheetNotFound:
        print("ERROR: The bot cannot find the sheet 'Daily_Job_Hunt'.")
        return
    except Exception as e:
        print(f"CONNECTION ERROR: {e}")
        return

    # Select columns (Added 'apply_link' which is the fixed column)
    cols = ['site', 'title', 'company', 'location', 'date_posted', 'apply_link']
    available_cols = [c for c in cols if c in df.columns]
    df = df[available_cols]
    
    try:
        sheet.clear()
        sheet.update([df.columns.values.tolist()] + df.values.tolist())
        print("Done. Jobs saved to Google Sheet.")
    except Exception as e:
        print(f"Error uploading data: {e}")

if __name__ == "__main__":
    queries = get_search_queries()
    print(f"Agent decided to search for: {queries}")
    
    df = run_scraper(queries)
    save_to_sheet(df)