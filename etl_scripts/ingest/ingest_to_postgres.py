import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
import glob
from datetime import datetime
import pickle
import json

class weadaptIngestion:
    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST', 'postgres'),  # Default to Docker service name
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'weadapt_dwh'),
            'user': os.getenv('DB_USER', 'weadapt'),
            'password': os.getenv('DB_PASSWORD', 'weadapt123')
        }
        self.engine = create_engine(
            f"postgresql://{self.db_config['user']}:{self.db_config['password']}@"
            f"{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
        )

    def load_excel(self, file_path):
        """Load Excel file"""
        return pd.read_excel(file_path)

    def load_pickle(self, file_path):
        """Load Pickle file"""
        with open(file_path, 'rb') as f:
            return pickle.load(f)

    def load_json(self, file_path):
        """Load JSON file"""
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        # Check the structure of the JSON
        if isinstance(data, dict):
            # If it's a dict, convert to DataFrame
            # Check if it needs transposition by examining structure
            df = pd.DataFrame(data)
            
            # If we have more columns than rows, it's likely transposed
            # But we need to check if the first key looks like a column name
            first_key = list(data.keys())[0]
            
            # If keys look like column names (not numeric), don't transpose
            if not str(first_key).isdigit():
                return df
            else:
                # Keys are numeric indices, transpose
                return df.T
        elif isinstance(data, list):
            return pd.DataFrame(data)
        else:
            return pd.DataFrame(data)

    def load_html_table(self, file_path):
        """Load HTML table"""
        return pd.read_html(file_path)[0]

    def load_parquet(self, file_path):
        """Load Parquet file"""
        return pd.read_parquet(file_path)

    def load_csv(self, file_path):
        """Load CSV file"""
        return pd.read_csv(file_path)

    def load_file(self, file_path):
        """Generic file loader based on extension"""
        ext = os.path.splitext(file_path)[1].lower()
        loaders = {
            '.xlsx': self.load_excel,
            '.pickle': self.load_pickle,
            '.pkl': self.load_pickle,
            '.json': self.load_json,
            '.html': self.load_html_table,
            '.parquet': self.load_parquet,
            '.csv': self.load_csv
        }
        if ext in loaders:
            return loaders[ext](file_path)
        else:
            raise ValueError(f"Unsupported file extension: {ext}")

    def ingest_dataframe(self, df, table_name, schema='staging'):
        """Generic dataframe ingestion"""
        try:
            # Add loaded_at timestamp
            df['loaded_at'] = datetime.now()

            # Clean column names
            df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('[^a-zA-Z0-9_]', '', regex=True)

            # Load to database
            df.to_sql(
                table_name,
                self.engine,
                schema=schema,
                if_exists='append',  # Changed to append for multiple files
                index=False
            )

            print(f"✓ Successfully loaded {len(df)} rows into {schema}.{table_name}")
            return True

        except Exception as e:
            print(f"✗ Error loading into {schema}.{table_name}: {str(e)}")
            return False

    def ingest_business_department(self, base_path):
        """Ingest Business Department data"""
        file_path = os.path.join(base_path, 'Business Department', 'product_list.xlsx')
        if os.path.exists(file_path):
            df = self.load_excel(file_path)
            return self.ingest_dataframe(df, 'staging_business_products')
        return False

    def ingest_customer_management(self, base_path):
        """Ingest Customer Management Department data"""
        results = []

        # Credit cards
        file_path = os.path.join(base_path, 'Customer Management Department', 'user_credit_card.pickle')
        if os.path.exists(file_path):
            df = self.load_pickle(file_path)
            results.append(self.ingest_dataframe(df, 'staging_customer_cards'))

        # Profiles
        file_path = os.path.join(base_path, 'Customer Management Department', 'user_data.json')
        if os.path.exists(file_path):
            df = self.load_json(file_path)
            # Remove this line - let the JSON keep its original column names
            # df.columns = [f'field_{i}' for i in range(len(df.columns))]
            # Drop columns that don't exist in the target table
            columns_to_drop = ['creation_date', 'city', 'birthdate', 'gender', 'device_address', 'user_type']
            existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]
            if existing_columns_to_drop:
                df = df.drop(columns=existing_columns_to_drop)
            results.append(self.ingest_dataframe(df, 'staging_customer_profiles'))

        # Jobs
        file_path = os.path.join(base_path, 'Customer Management Department', 'user_job.csv')
        if os.path.exists(file_path):
            df = self.load_csv(file_path)
            results.append(self.ingest_dataframe(df, 'staging_customer_jobs'))

        return all(results)

    def ingest_enterprise_department(self, base_path):
        """Ingest Enterprise Department data"""
        results = []

        # Merchants
        file_path = os.path.join(base_path, 'Enterprise Department', 'merchant_data.html')
        if os.path.exists(file_path):
            df = self.load_html_table(file_path)
            results.append(self.ingest_dataframe(df, 'staging_enterprise_merchants'))

        # Staff
        file_path = os.path.join(base_path, 'Enterprise Department', 'staff_data.html')
        if os.path.exists(file_path):
            df = self.load_html_table(file_path)
            results.append(self.ingest_dataframe(df, 'staging_enterprise_staff'))

        # Orders - multiple files
        order_files = [
            'order_with_merchant_data1.parquet',
            'order_with_merchant_data2.parquet',
            'order_with_merchant_data3.csv'
        ]
        for filename in order_files:
            file_path = os.path.join(base_path, 'Enterprise Department', filename)
            if os.path.exists(file_path):
                df = self.load_file(file_path)
                results.append(self.ingest_dataframe(df, 'staging_enterprise_orders'))

        return all(results)

    def ingest_marketing_department(self, base_path):
        """Ingest Marketing Department data"""
        results = []

        # Campaigns
        file_path = os.path.join(base_path, 'Marketing Department', 'campaign_data.csv')
        if os.path.exists(file_path):
            df = self.load_csv(file_path)
            # Fix malformed CSV - parse tab-separated data
            if len(df.columns) == 1:
                # Split the single column by tabs
                df = df.iloc[:, 0].str.split('\t', expand=True)
                # After splitting, we might have an empty first column due to leading tab
                df = df.iloc[:, 1:]  # Skip the first empty column
                df.columns = ['campaign_id', 'campaign_name', 'campaign_description', 'discount']
            results.append(self.ingest_dataframe(df, 'staging_marketing_campaigns'))

        # Transactions
        file_path = os.path.join(base_path, 'Marketing Department', 'transactional_campaign_data.csv')
        if os.path.exists(file_path):
            df = self.load_csv(file_path)
            results.append(self.ingest_dataframe(df, 'staging_marketing_transactions'))

        return all(results)

    def ingest_operations_department(self, base_path):
        """Ingest Operations Department data"""
        results = []

        # Line items prices - multiple files
        price_files = [
            'line_item_data_prices1.csv',
            'line_item_data_prices2.csv',
            'line_item_data_prices3.parquet'
        ]
        for filename in price_files:
            file_path = os.path.join(base_path, 'Operations Department', filename)
            if os.path.exists(file_path):
                df = self.load_file(file_path)
                results.append(self.ingest_dataframe(df, 'staging_operations_line_items_prices'))

        # Line items products - multiple files
        product_files = [
            'line_item_data_products1.csv',
            'line_item_data_products2.csv',
            'line_item_data_products3.parquet'
        ]
        for filename in product_files:
            file_path = os.path.join(base_path, 'Operations Department', filename)
            if os.path.exists(file_path):
                df = self.load_file(file_path)
                results.append(self.ingest_dataframe(df, 'staging_operations_line_items_products'))

        # Order headers - multiple files  ← UNINDENT THIS SECTION
        order_files = [
            'order_data_20200101-20200701.parquet',
            'order_data_20200701-20211001.pickle',
            'order_data_20211001-20220101.csv',
            'order_data_20220101-20221201.xlsx',
            'order_data_20221201-20230601.json',
            'order_data_20230601-20240101.html',
            'order_data_20240101-20241031.parquet'
        ]
        for filename in order_files:
            file_path = os.path.join(base_path, 'Operations Department', filename)
            if os.path.exists(file_path):
                df = self.load_file(file_path)
                # Handle JSON structure
                if filename.endswith('.json'):
                    # Check if we need to transpose based on actual structure
                    expected_columns = ['order_id', 'user_id', 'estimated_arrival', 'transaction_date']
                    if len(df.columns) != len(expected_columns) or not all(col in df.columns for col in expected_columns):
                        # If structure doesn't match expectations, try to identify columns or skip special handling
                        print(f"Warning: JSON file {filename} has unexpected structure. Using default column handling.")
                    else:
                        # JSON is 4 rows x many columns, transpose to columns
                        df = df.T.reset_index(drop=True)
                        df.columns = expected_columns
                # Ensure only expected columns for order headers (drop unnamed columns)
                expected_columns = ['order_id', 'user_id', 'estimated_arrival', 'transaction_date']
                df = df[[col for col in df.columns if col in expected_columns or not col.startswith('unnamed')]]
                # Keep only the expected columns if they exist
                existing_expected = [col for col in expected_columns if col in df.columns]
                if existing_expected:
                    df = df[existing_expected]
                results.append(self.ingest_dataframe(df, 'staging_operations_order_headers'))

        # Delivery delays
        file_path = os.path.join(base_path, 'Operations Department', 'order_delays.html')
        if os.path.exists(file_path):
            df = self.load_html_table(file_path)
            results.append(self.ingest_dataframe(df, 'staging_operations_delivery_delays'))

        return all(results)

    def run_all_ingestions(self, data_folder):
        """
        Ingest all weadapt datasets from raw sources
        """
        base_path = os.path.join(data_folder, 'Project Dataset')

        results = {
            'business': self.ingest_business_department(base_path),
            'customer_management': self.ingest_customer_management(base_path),
            'enterprise': self.ingest_enterprise_department(base_path),
            'marketing': self.ingest_marketing_department(base_path),
            'operations': self.ingest_operations_department(base_path)
        }

        return results

if __name__ == "__main__":
    ingestion = weadaptIngestion()
    results = ingestion.run_all_ingestions('../sql')  # Adjust path for container
    print("\n=== Ingestion Summary ===")
    for department, success in results.items():
        status = "✓ SUCCESS" if success else "✗ FAILED"
        print(f"{department}: {status}")

