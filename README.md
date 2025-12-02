# ShopZada Data Warehouse Project

## Overview

This project implements a comprehensive Data Warehouse (DWH) solution for ShopZada, featuring ETL pipelines, dimensional modeling, and business intelligence dashboards. The system follows Kimball methodology with a star schema design.

## Architecture

### 1. Data Architecture
- **Staging Layer**: Raw data ingestion and initial processing
- **Warehouse Layer**: Dimensional model with star schema
- **Presentation Layer**: Analytical views for business intelligence

### 2. Technology Stack
- **Database**: PostgreSQL
- **ETL Orchestration**: Apache Airflow
- **Data Processing**: Python (Pandas, SQLAlchemy)
- **Dashboard**: Flask + Plotly.js
- **Containerization**: Docker & Docker Compose

### 3. Data Flow
Raw Data → Staging → Warehouse (Dimensions & Facts) → Presentation Views → BI Dashboard

## Project Structure

```
dwh_finalproject_3CSC_group_WeAdapt/
├── sql/                          # Database schemas and views
│   ├── staging_schema.sql       # Staging layer tables
│   ├── warehouse_schema.sql     # Warehouse layer (star schema)
│   └── presentation_views.sql   # Analytical views
├── app/                          # Application code
│   ├── ingest_all_data.py       # Data ingestion scripts
│   ├── transform_load.py        # ETL transformation logic
│   ├── dashboard.py             # BI dashboard application
│   └── templates/
│       └── dashboard.html       # Dashboard UI
├── dags/                         # Airflow DAGs
│   └── etl_dag.py               # Main ETL pipeline
├── data/                         # Source data files
├── docker-compose.yml           # Multi-container setup
├── dockerfile                   # Container configuration
├── requirements.txt             # Python dependencies
└── init_db.sql                  # Database initialization
```

## Business Questions Addressed

1. **What kinds of campaigns drive the highest order volume?**
   - Analyzes campaign performance by order volume and revenue

2. **How do merchant performance metrics affect sales?**
   - Evaluates merchant revenue, order counts, and customer metrics

3. **What customer segments contribute most to revenue?**
   - Segments customers by job level and analyzes spending patterns

## Quick Start

### Prerequisites
- Docker and Docker Compose installed
- At least 4GB RAM available
- Ports 5433, 8080, 5000 available

### 1. Clone and Navigate
```bash
cd dwh_finalproject_3CSC_group_WeAdapt
```

### 2. Start the System
```bash
docker-compose up --build
```

This will start:
- PostgreSQL database (port 5433)
- Airflow services (port 8080)
- BI Dashboard (port 5000)

### 3. Initialize Data Pipeline

#### Option A: Manual Execution
```bash
# Run data ingestion
docker-compose exec airflow-worker python /opt/airflow/app/ingest_all_data.py

# Run transformations
docker-compose exec airflow-worker python /opt/airflow/app/transform_load.py
```

#### Option B: Airflow Web UI
1. Open http://localhost:8080
2. Login with: admin/admin
3. Enable and trigger the `shopzada_etl` DAG

### 4. Access the Dashboard
Open http://localhost:5000 to view the BI dashboard with:
- Key Performance Indicators (KPIs)
- Campaign performance charts
- Merchant analytics
- Customer segmentation
- Product performance
- Sales trends

## Data Sources

The system ingests data from multiple departments:

- **Business Department**: Product catalog (Excel)
- **Customer Management**: User profiles (CSV)
- **Marketing**: Campaigns and promotions (CSV)
- **Enterprise**: Orders, merchants, staff (HTML, Parquet)

## Database Schema

### Staging Layer
- `staging.stg_users`: Customer data
- `staging.stg_products`: Product catalog
- `staging.stg_campaigns`: Marketing campaigns
- `staging.stg_staff`: Employee data
- `staging.stg_orders`: Transaction data

### Warehouse Layer (Star Schema)
**Dimensions:**
- `warehouse.dim_time`: Date/time attributes
- `warehouse.dim_customer`: Customer profiles
- `warehouse.dim_product`: Product details
- `warehouse.dim_campaign`: Campaign information
- `warehouse.dim_staff`: Staff/merchant data

**Facts:**
- `warehouse.fact_sales`: Sales transactions
- `warehouse.fact_campaign_performance`: Campaign metrics

### Presentation Layer
Analytical views providing business insights:
- `vw_campaign_order_volume`
- `vw_merchant_performance`
- `vw_customer_segment_revenue`
- `vw_product_performance`
- `vw_sales_by_time`

## API Endpoints

The dashboard provides REST API endpoints:

- `GET /api/kpi_summary` - Key performance indicators
- `GET /api/campaign_order_volume` - Campaign analytics
- `GET /api/merchant_performance` - Merchant metrics
- `GET /api/customer_segments` - Customer segmentation
- `GET /api/product_performance` - Product analytics
- `GET /api/sales_trends` - Sales trends

## Monitoring and Maintenance

### Airflow Web UI
- Access at http://localhost:8080
- Monitor DAG runs and task status
- View logs and troubleshoot issues

### Database Access
```bash
# Connect to PostgreSQL
docker-compose exec shopzada-dw psql -U shopzada -d shopzada_dw

# Check data loading status
SELECT schemaname, tablename, n_tup_ins as rows
FROM pg_stat_user_tables
ORDER BY schemaname, tablename;
```

### Logs
```bash
# View container logs
docker-compose logs airflow-worker
docker-compose logs dashboard
```

## Troubleshooting

### Common Issues

1. **Port conflicts**: Ensure ports 5433, 8080, 5000 are available
2. **Memory issues**: Increase Docker memory allocation to 4GB+
3. **Data loading failures**: Check source files exist and are accessible
4. **Dashboard not loading**: Ensure warehouse data is populated first

### Reset System
```bash
# Stop and remove containers
docker-compose down -v

# Rebuild and restart
docker-compose up --build
```

## Project Deliverables

✅ **Architecture & Data Models**
- High-level DWH architecture diagram (documented above)
- Conceptual, logical, and physical data models (SQL schemas)
- Kimball methodology with star schema implementation

✅ **Data Workflow Design and Implementation**
- ETL pipeline with Airflow orchestration
- Python ingestion scripts with Pandas
- Transformation logic with SQLAlchemy
- Docker containerization
- Data quality validation

✅ **Analytical Layer**
- SQL views for business analytics
- OLAP-ready dimensional schema
- Business intelligence metrics

✅ **BI Dashboard**
- Interactive web dashboard with Plotly.js
- Real-time KPI monitoring
- Multiple chart types and visualizations
- RESTful API backend

## Methodology

This project follows the Kimball Lifecycle approach:

1. **Business Requirements Gathering** - Identified key business questions
2. **Data Architecture Design** - Star schema with conformed dimensions
3. **ETL Design and Development** - Automated pipelines with Airflow
4. **BI Application Development** - Interactive dashboard
5. **Deployment and Maintenance** - Docker containerization

## Future Enhancements

- Real-time data streaming integration
- Advanced analytics with machine learning
- Multi-cloud deployment options
- Enhanced security and access controls
- Automated testing and CI/CD pipelines

## Team

**WeAdapt Group** - 3CSC Data Warehouse Final Project

---

For technical support or questions, please refer to the code documentation and comments within the source files.
