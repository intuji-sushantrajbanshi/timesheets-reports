import os
import sys
import traceback
from datetime import datetime
import pandas as pd
import psycopg

def get_db_connection():
    """Establishes a direct connection to the Supabase PostgreSQL database."""
    try:
        conn_string = (
            f"host='{os.environ['DB_HOST']}' "
            f"dbname='{os.environ['DB_NAME']}' "
            f"user='{os.environ['DB_USER']}' "
            f"password='{os.environ['DB_PASS']}' "
            "sslmode='require'"
        )
        return psycopg.connect(conn_string)
    except Exception as e:
        print(f"❌ Critical Error: Could not connect to the database. Details: {e}", file=sys.stderr)
        sys.exit(1)

def fetch_project_time_report(conn, projects, date_filter, start_date, end_date):
    """Calls the get_project_time_report function in the database."""
    sql_query = """
        SELECT * FROM public.get_project_time_report(
            %s, -- target_project_names_param (array)
            %s, -- date_filter_param
            %s, -- custom_start_date_param
            %s  -- custom_end_date_param
        );
    """
    params = (projects, date_filter, start_date, end_date)
    
    try:
        print(f"🚀 Executing query for Projects: {projects}, Filter: '{date_filter}'...")
        # Use a server-side cursor for potentially large datasets
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            df = pd.DataFrame(cur.execute(sql_query, params).fetchall())
        
        if df.empty:
            print("✅ Query successful, but no data returned for the given filters.")
        else:
            print(f"✅ Successfully fetched {len(df)} rows.")
        return df

    except Exception as e:
        print(f"❌ An error occurred during database query: {e}", file=sys.stderr)
        traceback.print_exc()
        return pd.DataFrame()

def set_github_action_output(name, value):
    """Sets an output variable for subsequent GitHub Actions steps."""
    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            f.write(f"{name}={value}\n")

def main():
    """Main execution function."""
    projects_str = os.environ.get("TARGET_PROJECTS")
    if not projects_str:
        print("❌ Environment variable TARGET_PROJECTS is not set. Exiting.", file=sys.stderr)
        sys.exit(1)
        
    target_projects = [p.strip() for p in projects_str.split(',')]
    date_filter = os.environ.get("DATE_FILTER", "TODAY")
    custom_start_date = os.environ.get("CUSTOM_START_DATE") or None
    custom_end_date = os.environ.get("CUSTOM_END_DATE") or None

    conn = get_db_connection()
    final_df = fetch_project_time_report(conn, target_projects, date_filter, custom_start_date, custom_end_date)
    conn.close()

    output_dir = "exports"
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate a dynamic filename
    effective_filter = "custom-range" if custom_start_date else date_filter.lower().replace('_', '-')
    timestamp = datetime.now().strftime("%Y-%m-%d")
    output_filename = f"{output_dir}/report_{effective_filter}_{timestamp}.csv"

    if final_df.empty:
        print("⚠️ No data found. Creating an empty report file to indicate a successful run with no results.")
        # Create an empty file with headers to maintain schema consistency
        headers = [
            "date_filter_used", "filter_start_date", "filter_end_date", "project_name",
            "user_name", "focus_area", "description", "total_duration_raw",
            "total_hours", "total_entries"
        ]
        pd.DataFrame(columns=headers).to_csv(output_filename, index=False)
    else:
        final_df.to_csv(output_filename, index=False)
        print(f"✅ Successfully created report: {output_filename} with {len(final_df)} total rows.")

    # Set the path of the created file as an output for the next GitHub step
    set_github_action_output("csv_filepath", output_filename)

if __name__ == "__main__":
    main()
