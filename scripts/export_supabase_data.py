import os
import requests
import pandas as pd
import json
from datetime import datetime

# --- Configuration ---
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

# These will be set in the GitHub Actions workflow
# Example: 'Department of Health (Government of Western Australia),Coerco'
TARGET_PROJECTS_STR = os.environ.get("TARGET_PROJECTS", "")
# Example: 'TODAY', 'THIS_WEEK', 'LAST_MONTH'
DATE_FILTER = os.environ.get("DATE_FILTER", "TODAY") 

# Common headers for RPC calls
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

def call_project_report_rpc(project_name, date_filter):
    """Calls the get_project_time_report function in Supabase."""
    endpoint = f"{SUPABASE_URL}/rest/v1/rpc/get_project_time_report"
    payload = {
        "target_project_name_param": project_name,
        "date_filter_param": date_filter
    }
    
    try:
        print(f"🚀 Calling RPC for Project: '{project_name}', Filter: '{date_filter}'")
        response = requests.post(endpoint, headers=HEADERS, json=payload)
        
        response.raise_for_status() # Raises an HTTPError for bad responses (4xx or 5xx)
        
        data = response.json()
        if not data:
            print(f"✅ RPC successful, but no data returned for '{project_name}'.")
            return pd.DataFrame()
            
        df = pd.DataFrame(data)
        print(f"✅ Successfully fetched {len(df)} rows for '{project_name}'.")
        return df
        
    except requests.exceptions.HTTPError as http_err:
        print(f"❌ HTTP Error calling RPC for '{project_name}': {http_err.response.status_code} - {http_err.response.text}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ An unexpected error occurred calling RPC for '{project_name}': {str(e)}")
        return pd.DataFrame()

def main():
    """Main execution function."""
    if not TARGET_PROJECTS_STR:
        print("❌ Environment variable TARGET_PROJECTS is not set. Exiting.")
        # Create an error file to ensure the workflow step doesn't fail silently
        os.makedirs("exports", exist_ok=True)
        with open("exports/error.txt", "w") as f:
            f.write("TARGET_PROJECTS environment variable was not set.")
        exit(1)
        
    target_projects = [p.strip() for p in TARGET_PROJECTS_STR.split(',')]
    print(f"🎯 Target projects: {target_projects}")
    print(f"🗓️ Date filter: {DATE_FILTER}")
    
    all_project_dfs = []
    
    for project in target_projects:
        project_df = call_project_report_rpc(project, DATE_FILTER)
        if not project_df.empty:
            all_project_dfs.append(project_df)
    
    if not all_project_dfs:
        print("⚠️ No data found for any of the target projects. Creating an empty report.")
        # Create a placeholder file to indicate no data was found
        os.makedirs("exports", exist_ok=True)
        with open("exports/no_data_found.txt", "w") as f:
            f.write(f"No time entries found for projects {target_projects} with date filter '{DATE_FILTER}' on {datetime.now().isoformat()}")
        exit(0)

    # Combine all dataframes into one
    final_df = pd.concat(all_project_dfs, ignore_index=True)
    
    # Sort the final combined data
    final_df = final_df.sort_values(by=['project_name', 'user_name', 'focus_area'], ascending=[True, True, True])

    # --- Save to CSV ---
    os.makedirs("exports", exist_ok=True)
    
    # Generate a dynamic filename
    safe_date_filter = DATE_FILTER.lower().replace('_', '-')
    timestamp = datetime.now().strftime("%Y-%m-%d")
    output_filename = f"exports/report_{safe_date_filter}_{timestamp}.csv"
    
    final_df.to_csv(output_filename, index=False)
    
    print(f"✅ Successfully created report: {output_filename} with {len(final_df)} total rows.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ A critical error occurred in the main script execution: {str(e)}")
        import traceback
        os.makedirs("exports", exist_ok=True)
        with open("exports/fatal_error.txt", "w") as f:
            f.write(f"Error: {str(e)}\n\n{traceback.format_exc()}")
        exit(1)
