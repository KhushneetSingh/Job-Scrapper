import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import os

INPUT = "companies_input.xlsx"
OUTPUT = "../output/enriched_companies_output.xlsx"

# Helper: Guess Careers Page (simple heuristic)
def guess_careers_url(website_url):
    if pd.isna(website_url):
        return None
    careers_url = str(website_url).rstrip("/") + "/careers"
    return careers_url

# (Optional) Scrape job postings from a typical job-listing page like Greenhouse, Lever, etc.
def scrape_job_posts(jobs_page_url, max_jobs=3):
    results = []
    if pd.isna(jobs_page_url) or not str(jobs_page_url).startswith("http"):
        return results
    try:
        resp = requests.get(jobs_page_url, timeout=8)
        soup = BeautifulSoup(resp.text, "html.parser")
        jobs = soup.find_all("a", href=True)
        # Extremely naive example: just find first links that look like job postings
        for job in jobs:
            href = job.get("href")
            text = job.get_text(strip=True)
            if ("/job/" in href or "position" in href) and len(results) < max_jobs:
                results.append({"url": href, "title": text})
        return results
    except Exception:
        return results

def main():
    os.makedirs("output", exist_ok=True)
    df = pd.read_excel(INPUT)
    # Fill missing careers page URLs with a guess
    if "Careers Page URL" not in df.columns:
        df["Careers Page URL"] = None

    tqdm.pandas()
    df["Careers Page URL"] = df.apply(
        lambda row: row["Careers Page URL"] if pd.notna(row["Careers Page URL"])
        else guess_careers_url(row["Website URL"]),
        axis=1
    )

    # Optionally scrape up to 3 job posts for each careers page (can be slow!)
    # For production script, this section needs adjustment per company's actual jobs site.
    for idx, row in tqdm(df.iterrows(), total=df.shape[0]):
        jobs = scrape_job_posts(row["Careers Page URL"])
        for i, job in enumerate(jobs):
            df.at[idx, f"Job{i+1} URL"] = job["url"]
            df.at[idx, f"Job{i+1} Title"] = job["title"]

    df.to_excel(OUTPUT, index=False)
    print(f"Enriched data saved to {OUTPUT}")

if __name__ == "__main__":
    main()