import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import os

def extract_tvr_data(input_excel_filename):
    """
    Extract TVR values for regular and HD channels for both specified region and India.
    Returns a list [region_regular_tvr, region_hd_tvr, india_regular_tvr, india_hd_tvr] for mbs.py to use.
    """
    current_dir = os.getcwd()
    input_excel_path = os.path.join(current_dir, input_excel_filename)

    if not os.path.exists(input_excel_path):
        print(f"❌ Error: File '{input_excel_filename}' not found in {current_dir}")
        return []

    try:
        print(f"📄 Reading parameters from: {input_excel_path}")

        # ✅ Read needed cells
        sheet1 = pd.read_excel(input_excel_path, sheet_name='Property Details', header=None, usecols=[1], nrows=50)
        sheet2 = pd.read_excel(input_excel_path, sheet_name='Channel & Platform Details', header=None, usecols=[2], nrows=10)

        program = str(sheet1.iloc[0, 0]).strip() if sheet1.shape[0] > 0 else None
        region = str(sheet1.iloc[36, 0]).strip() if sheet1.shape[0] > 36 else None
        demographic = str(sheet1.iloc[35, 0]).strip() if sheet1.shape[0] > 35 else None
        time_period = str(sheet1.iloc[44, 0]).strip() if sheet1.shape[0] > 44 else None

        # Extract both regular and HD channel names
        channel_regular = str(sheet2.iloc[4, 0]).strip() if sheet2.shape[0] > 4 else None
        channel_hd = str(sheet2.iloc[5, 0]).strip() if sheet2.shape[0] > 5 else None

        # Combine channels for the query
        channels = f"{channel_regular},{channel_hd}" if channel_hd and str(channel_hd).lower() != 'nan' else channel_regular

        missing = []
        for key, value in [('Program', program), ('Region', region), ('Demographic', demographic),
                           ('Time Period', time_period), ('Channels', channels)]:
            if not value or value.lower() == 'nan':
                missing.append(key)

        if missing:
            print(f"❌ Error: Missing required fields: {', '.join(missing)}.")
            return []

        print(f"✅ Extracted:\n - Program: {program}\n - Region: {region}\n - Demographic: {demographic}\n"
              f" - Time Period: {time_period}\n - Channels: {channels}")
        print(f" - Regular Channel: {channel_regular}\n - HD Channel: {channel_hd or 'Not provided'}")

        # ✅ Parse time period
        if '-' in time_period:
            start_period, end_period = time_period.split('-')
        else:
            start_period = end_period = time_period

        start_period = ''.join(filter(str.isdigit, start_period))
        end_period = ''.join(filter(str.isdigit, end_period))

        if not start_period or not end_period:
            print(f"❌ Error: Invalid Time Period format '{time_period}'.")
            return []

        # ✅ SQL Connection
        server = 'MUMSQLP01113\\GRMINDSQL13'
        database = 'BARC_RATINGS'
        username = 'GRMINRatRO'
        password = 'GRMINRatRO'
        driver = 'ODBC Driver 17 for SQL Server'

        connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver.replace(' ', '+')}"

        print("🔗 Connecting to DB...")
        engine = create_engine(connection_string)

        # ✅ Function to extract TVR value for a specific channel
        def extract_tvr_for_channel(df, channel_name, region_name):
            if df.empty:
                print(f"⚠️ No data found for {channel_name} in {region_name}.")
                return 0

            # Filter for the specific channel
            channel_df = df[df['Channel'] == channel_name]

            if channel_df.empty:
                print(f"⚠️ No data found for {channel_name} in {region_name}.")
                return 0

            # Get the TVR value - assuming it's in the 'TVRs' column
            if 'TVRs' in channel_df.columns:
                tvr_value = channel_df['TVRs'].values[0]
                return tvr_value
            else:
                print(f"⚠️ TVRs column not found for {channel_name} in {region_name}.")
                return 0

        # ✅ Function to clean temporary tables
        def clean_temp_tables(connection):
            try:
                print("🧹 Cleaning up any existing temporary tables...")
                temp_tables = ['##temp_Channels', '##temp_Programs']
                for table in temp_tables:
                    cleanup_sql = f"IF OBJECT_ID('tempdb..{table}') IS NOT NULL DROP TABLE {table}"
                    with connection.begin():
                        connection.execute(text(cleanup_sql))
                        print(f"  - Cleaned up {table} if it existed")
                return True
            except Exception as e:
                print(f"⚠️ Cleanup warning (non-critical): {str(e)}")
                return False

        # ✅ Function to execute SQL safely with retries
        def execute_sql_with_retry(sql, region_name):
            max_attempts = 3
            for attempt in range(1, max_attempts+1):
                try:
                    with engine.connect() as connection:
                        clean_temp_tables(connection)
                        print(f"  Attempt {attempt} for {region_name} query...")
                        df = pd.read_sql_query(sql, connection)
                        return df
                except Exception as e:
                    print(f"  ⚠️ Attempt {attempt} failed: {str(e)}")
                    if attempt == max_attempts:
                        raise
                    print("  Retrying with a fresh connection...")
            return pd.DataFrame()

        # ✅ Query for specified region
        print(f"▶️ Querying for {region}...")
        sql_query_region = f"""
        exec [dbo].[Get_TVRs_For_Program_PR289PropOnePager]
            '{channels}',
            '{program}',
            '{region}',
            '{demographic}',
            {start_period},
            {end_period}
        """
        df_region = execute_sql_with_retry(sql_query_region, region)

        # Extract TVRs for both channel types in the region
        region_regular_tvr = extract_tvr_for_channel(df_region, channel_regular, region)
        region_hd_tvr = 0
        if channel_hd and str(channel_hd).lower() != 'nan':
            region_hd_tvr = extract_tvr_for_channel(df_region, channel_hd, region)

        print(f" Retrieved TVR for {channel_regular} in {region}: {region_regular_tvr}")
        if channel_hd and str(channel_hd).lower() != 'nan':
            print(f" Retrieved TVR for {channel_hd} in {region}: {region_hd_tvr}")

        # ✅ Query for India - explicit separate query
        print(f"▶️ Querying for India...")
        sql_query_india = f"""
        exec [dbo].[Get_TVRs_For_Program_PR289PropOnePager]
            '{channels}',
            '{program}',
            'India',
            '{demographic}',
            {start_period},
            {end_period}
        """
        df_india = execute_sql_with_retry(sql_query_india, "India")

        # Extract TVRs for both channel types in India
        india_regular_tvr = extract_tvr_for_channel(df_india, channel_regular, "India")
        india_hd_tvr = 0
        if channel_hd and str(channel_hd).lower() != 'nan':
            india_hd_tvr = extract_tvr_for_channel(df_india, channel_hd, "India")

        print(f" Retrieved TVR for {channel_regular} in India: {india_regular_tvr}")
        if channel_hd and str(channel_hd).lower() != 'nan':
            print(f" Retrieved TVR for {channel_hd} in India: {india_hd_tvr}")

        # Combine results - include HD TVRs if available
        all_tvrs = [region_regular_tvr, region_hd_tvr, india_regular_tvr, india_hd_tvr]

        # Export to Excel
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_program = program.replace(' ', '_').replace(',', '_')
        excel_file = f"{safe_program}_TVR_Data_{timestamp}.xlsx"

        print(f" Exporting TVRs to {excel_file}...")

        # Create export data with both channel types
        export_data = {
            'Region': [f"{region} ({channel_regular})",
                       f"{region} ({channel_hd})" if channel_hd and str(channel_hd).lower() != 'nan' else None,
                       f"India ({channel_regular})",
                       f"India ({channel_hd})" if channel_hd and str(channel_hd).lower() != 'nan' else None],
            'Channel': [channel_regular, channel_hd, channel_regular, channel_hd],
            'TVR_Value': all_tvrs
        }

        # Remove any None entries (in case HD channel wasn't provided)
        export_df = pd.DataFrame(export_data)
        export_df = export_df.dropna(subset=['Region'])

        export_df.to_excel(excel_file, index=False)
        print(f"✅ Saved TVR data: {excel_file}")

        return all_tvrs  # Returns [region_regular_tvr, region_hd_tvr, india_regular_tvr, india_hd_tvr]

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return []
    finally:
        if 'engine' in locals():
            engine.dispose()
            print("🔌 DB connection closed.")

if __name__ == "__main__":
    input_filename = input("Enter the Excel file name: ")
    tvrs = extract_tvr_data(input_filename)
    if tvrs:
        print(f"Extracted TVRs: {tvrs}")
    else:
        print("No TVRs extracted.")