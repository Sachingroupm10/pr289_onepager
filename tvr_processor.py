import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import os

def extract_tvr_data(input_excel_filename):
    """
        Extract one TVR value each from specified region and India.
        Returns a list [region_tvr, india_tvr] for mbs.py to use.
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
        channels = str(sheet2.iloc[4, 0]).strip() if sheet2.shape[0] > 4 else None

        missing = []
        for key, value in [('Program', program), ('Region', region), ('Demographic', demographic), ('Time Period', time_period), ('Channels', channels)]:
            if not value or value.lower() == 'nan':
                missing.append(key)
    
        if missing:
            print(f"❌ Error: Missing required fields: {', '.join(missing)}.")
            return []

        print(f"✅ Extracted:\n - Program: {program}\n - Region: {region}\n - Demographic: {demographic}\n - Time Period: {time_period}\n - Channels: {channels}")

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

        # ✅ Function to extract first TVR value from dataframe
        def extract_first_tvr(df, region_name):
            if df.empty:
                print(f"⚠️ No TVRs found for {region_name}.")
                return 0  # Return zero if no data

            tvr_col = next((col for col in df.columns if 'tvr' in col.lower()), None)
            if tvr_col:
                tvr_values = df[tvr_col].dropna().tolist()
            else:
                tvr_values = df.iloc[:, 0].dropna().tolist()

            if not tvr_values:
                print(f"⚠️ Query returned rows but no valid TVR values for {region_name}.")
                return 0

            return tvr_values[0]  # Return just the first value

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
        df_region = pd.read_sql_query(sql_query_region, engine)
        region_tvr = extract_first_tvr(df_region, region)
        print(f" Retrieved TVR for {region}: {region_tvr}")

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
        df_india = pd.read_sql_query(sql_query_india, engine)
        india_tvr = extract_first_tvr(df_india, "India")
        print(f" Retrieved TVR for India: {india_tvr}")

        # Combine results
        all_tvrs = [region_tvr, india_tvr]

        # Export to Excel
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_program = program.replace(' ', '_').replace(',', '_')
        excel_file = f"{safe_program}_TVR_Data_{timestamp}.xlsx"

        print(f" Exporting TVRs to {excel_file}...")

        export_df = pd.DataFrame({
            'Region': [region, 'India'],
            'TVR_Value': all_tvrs
        })
        export_df.to_excel(excel_file, index=False)
        print(f"✅ Saved TVR data: {excel_file}")

        return all_tvrs  # Returns [region_tvr, india_tvr]

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