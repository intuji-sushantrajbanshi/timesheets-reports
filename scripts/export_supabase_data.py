import os
import requests
import pandas as pd
import json
from datetime import datetime

# Configuration
supabase_url = os.environ["SUPABASE_URL"]
supabase_key = os.environ["SUPABASE_KEY"]
company_name = os.environ.get("COMPANY_NAME", "Timesheet App (Production)")
target_projects = os.environ.get("TARGET_PROJECTS", "").split(",")

# Common headers for all requests
headers = {
    "apikey": supabase_key,
    "Authorization": f"Bearer {supabase_key}",
    "Content-Type": "application/json",
    "Prefer": "return=representation"
}

def fetch_data(table_name, query_params=None, select_fields="*"):
    endpoint = f"{supabase_url}/rest/v1/{table_name}"
    
    params = {}
    if select_fields != "*":
        params["select"] = select_fields
        
    if query_params:
        params.update(query_params)
        
    try:
        print(f"🔍 Fetching from {endpoint} with params {params}")
        response = requests.get(endpoint, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            if not data:
                print(f"No data returned for {table_name}")
                return pd.DataFrame()
                
            df = pd.DataFrame(data)
            print(f"✅ Successfully fetched {len(df)} rows from {table_name}")
            return df
        else:
            print(f"❌ Error fetching {table_name}: {response.status_code} - {response.text}")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"❌ Error fetching {table_name}: {str(e)}")
        return pd.DataFrame()

try:
    # Get company ID
    print(f"🔍 Fetching company ID for {company_name}")
    company_df = fetch_data("Company", {"name": f"eq.{company_name}"}, "id")
    
    if company_df.empty or "id" not in company_df.columns:
        print(f"❌ Company '{company_name}' not found")
        with open("exports/error.txt", "w") as f:
            f.write(f"Company '{company_name}' not found")
        exit(1)
        
    company_id = company_df.iloc[0]["id"]
    print(f"✅ Company ID: {company_id}")
    
    # Fetch data from required tables with company filter
    print("📊 Fetching time entries...")
    time_entries_df = fetch_data(
        "TimeEntry",
        {
            "companyId": f"eq.{company_id}",
            "deletedAt": "is.null"
        }
    )
    
    print("📊 Fetching projects...")  
    projects_df = fetch_data(
        "Project",
        {
            "companyId": f"eq.{company_id}",
            "deletedAt": "is.null"
        }
    )
    
    print("📊 Fetching users...")
    users_df = fetch_data(
        "User",
        {
            "companyId": f"eq.{company_id}",
            "deletedAt": "is.null"
        }
    )
    
    if time_entries_df.empty or projects_df.empty or users_df.empty:
        print("❌ One or more required tables are empty")
        with open("exports/error.txt", "w") as f:
            f.write("One or more required tables are empty")
        exit(1)
    
    # Print column names for debugging
    print(f"📋 TimeEntry columns: {list(time_entries_df.columns)}")
    print(f"📋 Project columns: {list(projects_df.columns)}")
    print(f"📋 User columns: {list(users_df.columns)}")
    
    # Find column names (case-insensitive matching)
    def find_column(df, possible_names):
        df_columns = [col.lower() for col in df.columns]
        for name in possible_names:
            if name.lower() in df_columns:
                return df.columns[df_columns.index(name.lower())]
        return None
    
    # Map column names for projects
    project_id_col = find_column(projects_df, ["id"])
    project_title_col = find_column(projects_df, ["title"])
    
    # Map column names for users
    user_id_col = find_column(users_df, ["id"])
    user_first_name_col = find_column(users_df, ["firstName"])
    user_last_name_col = find_column(users_df, ["lastName"])
    user_email_col = find_column(users_df, ["email"])
    
    # Map column names for time entries
    time_project_id_col = find_column(time_entries_df, ["projectId"])
    time_user_id_col = find_column(time_entries_df, ["userId"])
    time_duration_col = find_column(time_entries_df, ["duration"])
    time_entry_date_col = find_column(time_entries_df, ["entryDate", "created_at"])
    
    print(f"🔍 Column mapping:")
    print(f"   Project: id={project_id_col}, title={project_title_col}")
    print(f"   User: id={user_id_col}, first={user_first_name_col}, last={user_last_name_col}, email={user_email_col}")
    print(f"   Time: project_id={time_project_id_col}, user_id={time_user_id_col}, duration={time_duration_col}")
    
    # Filter projects for only the target projects
    if not target_projects or not target_projects[0]:
        print("⚠️ No target projects specified, using all projects")
        projects_filtered = projects_df.copy()
    else:
        print(f"🔍 Filtering projects: {target_projects}")
        projects_filtered = projects_df[
            projects_df[project_title_col].isin(target_projects)
        ]
    
    if projects_filtered.empty:
        print("❌ Target projects not found")
        print(f"Available projects: {projects_df[project_title_col].tolist()}")
        with open("exports/error.txt", "w") as f:
            f.write(f"Target projects not found. Available: {projects_df[project_title_col].tolist()}")
        exit(1)
    
    print(f"✅ Found {len(projects_filtered)} target projects")
    
