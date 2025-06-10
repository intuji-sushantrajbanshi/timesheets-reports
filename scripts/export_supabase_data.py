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
    
    # Merge time entries with filtered projects
    merged_df = time_entries_df.merge(
        projects_filtered[[project_id_col, project_title_col]], 
        left_on=time_project_id_col, 
        right_on=project_id_col, 
        how="inner"
    )
    
    # Merge with users
    user_cols_to_merge = [user_id_col]
    if user_first_name_col: user_cols_to_merge.append(user_first_name_col)
    if user_last_name_col: user_cols_to_merge.append(user_last_name_col)
    if user_email_col: user_cols_to_merge.append(user_email_col)
    
    merged_df = merged_df.merge(
        users_df[user_cols_to_merge], 
        left_on=time_user_id_col, 
        right_on=user_id_col, 
        how="inner"
    )
    
    if merged_df.empty:
        print("❌ No data after merging tables")
        with open("exports/error.txt", "w") as f:
            f.write("No data after merging tables")
        exit(1)
    
    print(f"✅ Successfully merged data: {len(merged_df)} time entries")
    
    # Create user name column
    if user_first_name_col and user_last_name_col:
        merged_df["user_name"] = (
            merged_df[user_first_name_col].fillna("").astype(str) + " " + 
            merged_df[user_last_name_col].fillna("").astype(str)
        ).str.strip()
    else:
        merged_df["user_name"] = merged_df[user_email_col] if user_email_col else "Unknown User"
    
    # Calculate duration
    if time_duration_col and time_duration_col in merged_df.columns:
        merged_df["duration_minutes"] = pd.to_numeric(merged_df[time_duration_col], errors="coerce").fillna(0)
    else:
        # Calculate from start/end times if available
        start_col = find_column(time_entries_df, ["startTime"])
        end_col = find_column(time_entries_df, ["endTime"])
        
        if start_col and end_col:
            merged_df["duration_minutes"] = (
                pd.to_datetime(merged_df[end_col]) - 
                pd.to_datetime(merged_df[start_col])
            ).dt.total_seconds() / 60
        else:
            merged_df["duration_minutes"] = 0
            print("⚠️ No duration column found, using 0 for all entries")
    
    # Group by project and user
    group_cols = [project_title_col, "user_name"]
    if user_email_col:
        group_cols.append(user_email_col)
    
    agg_dict = {
        "duration_minutes": ["sum", "count"]
    }
    
    if time_entry_date_col:
        agg_dict[time_entry_date_col] = ["min", "max"]
    
    grouped = merged_df.groupby(group_cols).agg(agg_dict).reset_index()
    
    # Flatten column names
    new_columns = []
    for col in grouped.columns:
        if isinstance(col, tuple):
            if col[1]:
                new_columns.append(f"{col[0]}_{col[1]}")
            else:
                new_columns.append(col[0])
        else:
            new_columns.append(col)
    grouped.columns = new_columns
    
    # Rename columns to match expected format
    column_mapping = {
        project_title_col: "project",
        "user_name": "user_name",
        "duration_minutes_sum": "total_duration_raw",
        "duration_minutes_count": "total_entries"
    }
    
    if user_email_col:
        column_mapping[user_email_col] = "user_email"
    
    if time_entry_date_col:
        column_mapping[f"{time_entry_date_col}_min"] = "first_entry_date"
        column_mapping[f"{time_entry_date_col}_max"] = "last_entry_date"
    
    grouped = grouped.rename(columns=column_mapping)
    
    # Add total_hours column
    grouped["total_hours"] = grouped["total_duration_raw"]
    
    # Add missing columns if not present
    required_cols = ["project", "user_name", "user_email", "total_duration_raw", "total_hours", "total_entries", "first_entry_date", "last_entry_date"]
    for col in required_cols:
        if col not in grouped.columns:
            grouped[col] = "" if col in ["user_email", "first_entry_date", "last_entry_date"] else 0
    
    # Sort data: Department of Health first (by total_hours desc), then separator, then Coerco (by total_hours desc)
    dept_health = grouped[grouped["project"] == "Department of Health (Government of Western Australia)"].copy()
    coerco = grouped[grouped["project"] == "Coerco"].copy()
    
    # Sort each project group by total_hours descending
    dept_health = dept_health.sort_values("total_hours", ascending=False)
    coerco = coerco.sort_values("total_hours", ascending=False)
    
    # Create final report with separator
    final_rows = []
    
    # Add Department of Health data
    for _, row in dept_health.iterrows():
        final_rows.append(row.to_dict())
    
    # Add separator row
    separator_row = {
        "project": "--- SEPARATOR ---",
        "user_name": "",
        "user_email": "",
        "total_duration_raw": None,
        "total_hours": None,
        "total_entries": None,
        "first_entry_date": None,
        "last_entry_date": None
    }
    final_rows.append(separator_row)
    
    # Add Coerco data
    for _, row in coerco.iterrows():
        final_rows.append(row.to_dict())
    
    # Create final DataFrame
    final_df = pd.DataFrame(final_rows)
    
    # Ensure column order matches the expected format
    column_order = ["project", "user_name", "user_email", "total_duration_raw", "total_hours", "total_entries", "first_entry_date", "last_entry_date"]
    final_df = final_df[column_order]
    
    # Save to CSV
    final_df.to_csv("exports/project_time_report.csv", index=False)
    print(f"✅ Created project time report with {len(final_df)} rows")
    
    # Create summary
    summary = {
        "export_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "company_id": company_id,
        "total_rows": len(final_df),
        "dept_health_users": len(dept_health),
        "coerco_users": len(coerco)
    }
    
    with open("exports/export_summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    
    exit(0)
        
except Exception as e:
    print(f"❌ Error creating project time report: {str(e)}")
    import traceback
    traceback.print_exc()
    with open("exports/error.txt", "w") as f:
        f.write(f"Error: {str(e)}\n\n{traceback.format_exc()}")
    exit(1)
