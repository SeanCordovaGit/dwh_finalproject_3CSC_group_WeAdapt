from flask import Flask, render_template, jsonify
import pandas as pd
from sqlalchemy import create_engine
import plotly
import plotly.express as px
import plotly.graph_objects as go
import json
import logging
import os

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_engine():
    """Create database engine for PostgreSQL"""
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5433')
    db_name = os.getenv('DB_NAME', 'shopzada_dw')
    db_user = os.getenv('DB_USER', 'shopzada')
    db_password = os.getenv('DB_PASSWORD', 'shopzada')
    return create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

def get_campaign_order_volume():
    """Get campaign order volume data"""
    engine = get_db_engine()
    query = "SELECT * FROM presentation.vw_campaign_order_volume LIMIT 10"
    df = pd.read_sql(query, engine)
    return df

def get_merchant_performance():
    """Get merchant performance data"""
    engine = get_db_engine()
    query = "SELECT * FROM presentation.vw_merchant_performance LIMIT 10"
    df = pd.read_sql(query, engine)
    return df

def get_customer_segment_revenue():
    """Get customer segment revenue data"""
    engine = get_db_engine()
    query = "SELECT * FROM presentation.vw_customer_segment_revenue LIMIT 10"
    df = pd.read_sql(query, engine)
    return df

def get_product_performance():
    """Get product performance data"""
    engine = get_db_engine()
    query = "SELECT * FROM presentation.vw_product_performance LIMIT 10"
    df = pd.read_sql(query, engine)
    return df

def get_sales_by_time():
    """Get sales by time data"""
    engine = get_db_engine()
    query = "SELECT * FROM presentation.vw_sales_by_time ORDER BY year_actual, quarter_actual"
    df = pd.read_sql(query, engine)
    return df

@app.route('/')
def dashboard():
    """Main dashboard page"""
    return render_template('dashboard.html')

@app.route('/api/campaign_order_volume')
def api_campaign_order_volume():
    """API endpoint for campaign order volume"""
    try:
        df = get_campaign_order_volume()
        fig = px.bar(df, x='campaign_name', y='total_orders',
                    title='Campaign Order Volume',
                    labels={'campaign_name': 'Campaign', 'total_orders': 'Total Orders'})
        graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        return jsonify({'data': graphJSON})
    except Exception as e:
        logger.error(f"Error getting campaign data: {e}")
        return jsonify({'error': str(e)})

@app.route('/api/merchant_performance')
def api_merchant_performance():
    """API endpoint for merchant performance"""
    try:
        df = get_merchant_performance()
        fig = px.bar(df, x='merchant_name', y='total_revenue',
                    title='Merchant Performance by Revenue',
                    labels={'merchant_name': 'Merchant', 'total_revenue': 'Total Revenue'})
        graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        return jsonify({'data': graphJSON})
    except Exception as e:
        logger.error(f"Error getting merchant data: {e}")
        return jsonify({'error': str(e)})

@app.route('/api/customer_segments')
def api_customer_segments():
    """API endpoint for customer segments"""
    try:
        df = get_customer_segment_revenue()
        fig = px.bar(df, x='job_level', y='total_revenue',
                    title='Revenue by Customer Segments',
                    labels={'job_level': 'Job Level', 'total_revenue': 'Total Revenue'})
        graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        return jsonify({'data': graphJSON})
    except Exception as e:
        logger.error(f"Error getting customer segment data: {e}")
        return jsonify({'error': str(e)})

@app.route('/api/product_performance')
def api_product_performance():
    """API endpoint for product performance"""
    try:
        df = get_product_performance()
        fig = px.bar(df, x='product_name', y='total_revenue',
                    title='Top Products by Revenue',
                    labels={'product_name': 'Product', 'total_revenue': 'Total Revenue'})
        graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        return jsonify({'data': graphJSON})
    except Exception as e:
        logger.error(f"Error getting product data: {e}")
        return jsonify({'error': str(e)})

@app.route('/api/sales_trends')
def api_sales_trends():
    """API endpoint for sales trends"""
    try:
        df = get_sales_by_time()
        fig = px.line(df, x='month_name', y='total_revenue',
                     title='Sales Trends by Month',
                     labels={'month_name': 'Month', 'total_revenue': 'Total Revenue'})
        graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        return jsonify({'data': graphJSON})
    except Exception as e:
        logger.error(f"Error getting sales trends: {e}")
        return jsonify({'error': str(e)})

@app.route('/api/kpi_summary')
def api_kpi_summary():
    """API endpoint for KPI summary"""
    try:
        engine = get_db_engine()

        # Get key metrics
        queries = {
            'total_orders': "SELECT COUNT(*) as value FROM warehouse.fact_sales",
            'total_revenue': "SELECT SUM(total_amount) as value FROM warehouse.fact_sales",
            'unique_customers': "SELECT COUNT(DISTINCT customer_key) as value FROM warehouse.fact_sales",
            'avg_order_value': "SELECT AVG(total_amount) as value FROM warehouse.fact_sales"
        }

        kpis = {}
        for metric, query in queries.items():
            result = pd.read_sql(query, engine)
            kpis[metric] = float(result['value'].iloc[0]) if not result.empty else 0

        return jsonify(kpis)
    except Exception as e:
        logger.error(f"Error getting KPIs: {e}")
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
