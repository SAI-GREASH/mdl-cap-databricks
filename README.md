
# MDL-CAP | Marketing Data Lake & Campaign Activation Platform

## Project Overview
A hands-on Databricks data engineering project demonstrating a
Medallion Architecture using synthetic marketing campaign data.

## Architecture
Synthetic Marketing Data (CSV)
    ↓
Unity Catalog Volume (Landing)
    ↓
Bronze: Incremental ingestion using Auto Loader
    ↓
Silver: Cleaning, validation and quarantine
    ↓
Gold: Daily channel analytics and KPI calculations
    ↓
Databricks AI/BI Dashboard

## Marketing Data Sources
- Google Ads
- YouTube Ads
- Facebook Ads
- Internal Marketing

## Technology Stack
- Databricks
- Python and PySpark
- Lakeflow Spark Declarative Pipelines
- Delta Lake
- Unity Catalog
- Databricks SQL
- AI/BI Dashboards
- GitHub

## Validated Results
- 1,200 synthetic campaign records processed
- 4 marketing sources
- 30 days of data: October 1–30, 2026
- 1,200 valid Silver records
- 0 quarantined records
- 120 Gold daily-channel records
- KPI reconciliation checks passed

## Dashboard
Published dashboard: MDL-CAP | Marketing Campaign Performance

Features:
- Total spend, impressions, clicks and conversions
- Channel-level performance comparisons
- Daily spending trends
- Click-through-rate analysis
- Marketing channel and date-range filters

## Project Status
Completed:
- Synthetic data generation
- Bronze, Silver and Gold processing
- Data quality validation
- Interactive dashboard and publication
- GitHub version control

Planned:
- Automated testing
- Databricks deployment automation
- GitHub Actions CI/CD
- Pipeline monitoring and recovery procedures

## Disclaimer
All advertising data is fictional and generated for learning.
No actual marketing platforms are connected.
Campaign activation/export is not yet implemented.
