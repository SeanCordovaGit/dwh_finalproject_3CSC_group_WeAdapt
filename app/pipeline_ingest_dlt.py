import dlt
import pandas as pd

# BUSINESS
@dlt.resource
def products():
    yield pd.read_csv("Business/product_list.csv")

# OPERATIONS
@dlt.resource
def line_items_prices():
    df1 = pd.read_csv("Operations/line_item_data_prices1.csv")
    df2 = pd.read_csv("Operations/line_item_data_prices2.csv")
    yield pd.concat([df1, df2], ignore_index=True)

@dlt.resource
def line_items_products():
    df1 = pd.read_csv("Operations/line_item_data_products1.csv")
    df2 = pd.read_csv("Operations/line_item_data_products2.csv")
    yield pd.concat([df1, df2], ignore_index=True)

@dlt.resource
def orders():
    yield pd.read_excel("Operations/order_data.xlsx")

@dlt.resource
def order_delays():
    yield pd.read_csv("Operations/order_delays.csv")

# CUSTOMER
@dlt.resource
def users():
    yield pd.read_csv("Customer/user_job.csv")

# ENTERPRISE
@dlt.resource
def merchants():
    yield pd.read_parquet("Enterprise/merchant_data.parquet")

@dlt.resource
def staff():
    yield pd.read_parquet("Enterprise/staff_data.parquet")

@dlt.resource
def order_merchant():
    yield pd.read_csv("Enterprise/order_with_merchant_data3.csv")

# MARKETING
@dlt.resource
def campaigns():
    yield pd.read_csv("Marketing/campaign_data.csv")

@dlt.resource
def transactional_campaigns():
    yield pd.read_csv("Marketing/transactional_campaign_data.csv")

# PIPELINE
pipeline = dlt.pipeline(
    pipeline_name="shopzada_pipeline",
    destination="duckdb",
    dataset_name="bronze"
)

pipeline.run([
    products(),
    line_items_prices(),
    line_items_products(),
    orders(),
    order_delays(),
    users(),
    merchants(),
    staff(),
    order_merchant(),
    campaigns(),
    transactional_campaigns()
])