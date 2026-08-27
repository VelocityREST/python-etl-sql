import marimo

__generated_with = "0.23.16"
app = marimo.App(width="full")

with app.setup:
    # Initialization code that runs before all other cells
    import requests
    import time
    from io import StringIO
    import pandas as pd
    import sqlite3
    from datetime import datetime
    from zoneinfo import ZoneInfo

    headers = {
        "Authorization": "put your token here",
        "Content-Type": "application/json",
    }

    jobsTable_create_statement = """CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT,
        job_posting_id INTEGER,
        job_title TEXT,
        company_name TEXT,
        company_id INTEGER,
        job_location TEXT,
        job_summary TEXT,
        job_description_formatted TEXT,
        job_seniority_level TEXT,
        job_function TEXT,
        job_employment_type TEXT,
        job_industries TEXT,
        company_url TEXT,
        posted_date TEXT,
        job_num_applicants INTEGER,
        apply_link TEXT,
        title_id INTEGER,
        job_posted_date TEXT,
        job_poster_name TEXT,
        job_poster_title TEXT,
        job_poster_url TEXT,
        base_salary_min REAL,
        base_salary_max REAL,
        base_salary_period TEXT,
        job_base_pay_range TEXT,
        timestamp TEXT,
        query TEXT,
        location TEXT)"""


@app.cell
def _():
    # pd.options.mode.dtype_backend = 'pyarrow' (Removed, not valid in Pandas 3.0)
    return


@app.function
def send_request(keyword, location, jobids_to_negate):
    url = "https://api.brightdata.com/datasets/v3/trigger"
    params = {
        "dataset_id": "gd_lpfll7v5hcqtkxl6l",
        "include_errors": "true",
        "type": "discover_new",
        "discover_by": "keyword",
    }
    data = (
        {
            "location": location,
            "keyword": keyword,
            "country": "CA",
            "time_range": "Past 24 hours",
            "job_type": "",
            "experience_level": "",
            "remote": "",
            "company": "",
            # "location_radius":"50 miles (80 km)",
            "location_radius": "",
            "selective_search": True,
            "jobs_to_not_include": jobids_to_negate
        },
    )
    response = requests.post(
        url=url, headers=headers, params=params, json=data
    )
    snapshot_id = response.json().get("snapshot_id")
    print(f"snapshot id: {snapshot_id}")
    return snapshot_id


@app.function
def check_snapshot_status(snapshot_id):
    progress_url = "https://api.brightdata.com/datasets/v3/progress/"
    progress = progress_url + snapshot_id
    snapshot_status = requests.get(progress, headers=headers)
    # print(snapshot_status.json())
    return snapshot_status.json()


@app.function
# Helper functions to send, check the status of the requests and get them
def get_results(snapshot_id):
    results_url = "https://api.brightdata.com/datasets/v3/snapshot/"
    snapshot_url = results_url + snapshot_id
    params = {
        "format": "json",
    }
    while True:
        results = requests.get(snapshot_url, headers=headers, params=params).text
        # if isinstance(results, dict) and results.get('status') == 'building':
        if '"status":"building"' in results:
            print(f"get_results is not ready, it returned {results}")
            time.sleep(30)
            # results = requests.get(snapshot_url, headers=headers, params=params).text
        else: return results


@app.function
def scrape_keyword(keyword, location, jobids_to_negate):
    print(f"# Sending request for {keyword} in {location}")
    snapid= send_request(keyword, location, jobids_to_negate)
    start_time = time.time()
    while True:
        if check_snapshot_status(snapid)["status"] == 'ready':
            print(f"## Results received")
            return get_results(snapid)
        elif time.time() - start_time > 600:
            raise TimeoutError("## Execution took too long (over 600 sec)")
        else:
            print("## Snapshot not ready, will try again in 30 seconds.")
            time.sleep(30)


@app.function(hide_code=True)
def tweak_processing_df(df):
    return (
        df.reindex(
            columns=[
                "url",
                "job_posting_id",
                "job_title",
                "company_name",
                "company_id",
                "job_location",
                "job_summary",
                "applay_link",
                "job_seniority_level",
                "job_function",
                "job_employment_type",
                "job_industries",
                "job_base_pay_range",
                "company_url",
                "job_posted_time",
                "job_num_applicants",
                "discovery_input",
                "apply_link",
                "country_code",
                "title_id",
                "company_logo",
                "job_posted_date",
                "job_poster",
                "application_availability",
                "job_description_formatted",
                "selective_search",
                "base_salary",
                "salary_standards",
                "timestamp",
                "input",
                "error",
                "error_code",
                "warning",
                "warning_code",
            ]
        )
        .assign(
            query=lambda _df: _df["discovery_input"].astype(object).str.get("keyword")
        )
        .assign(
            location=lambda _df: _df["discovery_input"].astype(object).str.get("location")
        )
        .assign(
            posted_date=lambda _df: pd.to_datetime(
                _df["job_posted_date"]
            ).dt.tz_convert("America/Toronto")
        )
        # .assign(
        #     job_posted_date=lambda _df: pd.to_datetime(_df["job_posted_date"])
        # )
        .assign(
            timestamp=lambda _df: pd.to_datetime(
                _df["timestamp"]
            ).dt.tz_convert("America/Toronto")
        )
        .assign(
            job_poster_name=lambda _df: _df.job_poster.astype(object).str.get("name")
        )
        .assign(
            job_poster_title=lambda _df: _df.job_poster.astype(object).str.get("title")
        )
        .assign(
            job_poster_url=lambda _df: _df.job_poster.astype(object).str.get("url")
        )
        .assign(
            base_salary_min=lambda _df: _df.base_salary.astype(object).str.get("min_amount")
        )
        .assign(
            base_salary_max=lambda _df: _df.base_salary.astype(object).str.get("max_amount")
        )
        .assign(
            base_salary_period=lambda _df: _df.base_salary.astype(object).str.get("payment_period")
        )
        .loc[
            :,
            [
                "url",
                "job_posting_id",
                "job_title",
                "company_name",
                "company_id",
                "job_location",
                "job_summary",
                "job_description_formatted",
                "job_seniority_level",
                "job_function",
                "job_employment_type",
                "job_industries",
                "company_url",
                "posted_date",
                "job_num_applicants",
                "apply_link",
                "title_id",
                # "job_posted_date",
                "job_poster_name",
                "job_poster_title",
                "job_poster_url",
                "base_salary_min",
                "base_salary_max",
                "base_salary_period",
                "job_base_pay_range",
                "timestamp",
                "query",
                "location",
            ],
        ]
    )


@app.function
def retrieve_jobids_window(days):
    with sqlite3.connect("jobscrape.db") as conn:
        cursor = conn.cursor()
        cursor.execute(jobsTable_create_statement)
        time_modifier = f"-{days} days"
        cursor.execute(
            "select job_posting_id from jobs where posted_date >= datetime('now', ?)",
            (time_modifier,)
        )
        results_list = cursor.fetchall()
        results = [row[0] for row in results_list]
        print(f"Total of {len(results)} was returned to be negated")
    return results


@app.function
def insert_into_db(df):
    with sqlite3.connect("jobscrape.db") as conn:
        cursor = conn.cursor()
        cursor.execute(jobsTable_create_statement)
        df.to_sql("jobs", conn, if_exists="append", index = False)
        conn.commit


@app.cell
def _():
    # Scraping Logic
    ## A list of keywords and locations to go throw
    keywords = ["data analyst", "business analyst", "data engineer", "data scientist"]
    locations = ["toronto", "mississauga", "brampton", "oakville", "north york", "scarborough", "vaughan", "richmond hill", "caledon"]
    now = datetime.now(ZoneInfo("America/Toronto")).isoformat()
    print(f"Time at the start running the script {now}")
    ## get a list of job ids to negate before the query is sent
    jobids_to_negate = retrieve_jobids_window(2)
    for keyword in keywords:
        for loc in locations:
            raw_results = scrape_keyword(keyword, loc, jobids_to_negate)
            print(raw_results)

            clean_results = raw_results.strip() if isinstance(raw_results, str) else str(raw_results).strip()

            if clean_results not in ['Snapshot is empty', '[]', '']:
                # Processing results into df
                processed_df = tweak_processing_df(pd.read_json(StringIO(clean_results)))
                print(f"{processed_df.shape[0]} results returned from scrapping")
                insert_into_db(processed_df)
                print("Data inserted into DB")
                # Updating negated list
                jobids_to_negate = retrieve_jobids_window(2)
            else:
                print("Snapshot is empty or returned '[]', moving to the next request") 
        print("_________________________________")
    now = datetime.now(ZoneInfo("America/Toronto")).isoformat()
    print(f"Time at the end running the script {now}")
    return


if __name__ == "__main__":
    app.run()
