import pandas as pd
import argparse
from sqlalchemy import create_engine
from azure.storage.blob import ContainerClient

def main():
    # Hard-coded date range
    start_date = '2023-01-01'
    end_date = '2024-12-31'
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Feature engineering script for Octopus Energy data')
    parser.add_argument('--host', required=True, help='PostgreSQL host')
    parser.add_argument('--port', type=int, default=5432, help='PostgreSQL port')
    parser.add_argument('--dbname', required=True, help='Database name')
    parser.add_argument('--user', required=True, help='Database username')
    parser.add_argument('--password', required=True, help='Database password')
    parser.add_argument('--output_path', required=True, help='Path to save the processed data')
    parser.add_argument('--azure_blob_conn_str', required=True, help='azure_blob_conn_str')
    
    args = parser.parse_args()
    
    # Connect to postgres
    pg_connection_string = f"postgresql://{args.user}:{args.password}@{args.host}:{args.port}/{args.dbname}"
    engine = create_engine(pg_connection_string)
    
    # Select data - aggregate hourly consumption and join with weather
    query = f"""
    SELECT 
        date_trunc('hour', e.interval_start) as hour_start,
        SUM(e.consumption) as consumption,
        w.temperature_2m,
        w.precipitation
    FROM 
        public.octopus_electricity e
    LEFT JOIN 
        public.historic_weather w ON date_trunc('hour', e.interval_start) = w.time_start
    WHERE 
        e.interval_start BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY 
        date_trunc('hour', e.interval_start), w.temperature_2m, w.precipitation
    ORDER BY 
        date_trunc('hour', e.interval_start) ASC
    """
    
    df = pd.read_sql(query, engine)
    
    # Feature engineering
    # Create time-based features
    df['day_of_week'] = pd.to_datetime(df['hour_start']).dt.dayofweek
    
    
    # Temperature features
    if 'temperature_2m' in df.columns:
        df['temperature_squared'] = df['temperature_2m'] ** 2
    
    # Drop rows with NaN values (from lag and rolling calculations)
    df = df.dropna()
    
    # Write to temporary location
    output = df.to_csv (index_label="idx", encoding = "utf-8")
    #print(output)
    #print(args.azure_blob_conn_str)
    blob_block = ContainerClient.from_connection_string(
    conn_str=args.azure_blob_conn_str,
    container_name='octopusenergy'
    )
    blob_block.upload_blob(args.output_path, output, overwrite=True, encoding='utf-8')
    print(f"Feature engineering complete. Data saved to {args.output_path}")

if __name__ == "__main__":
    main()