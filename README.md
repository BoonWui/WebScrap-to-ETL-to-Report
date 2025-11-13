# WebScrap-to-ETL-to-Report
A project to scrap data from web then ETL and then to report

Futures Position ETL & Charting Tool

This project is a Python-based ETL and reporting tool for futures position data. It demonstrates how to extract data from structured sources, transform it into a clean format, and generate charts for analysis.

⚠️ Note: This repository does not include any proprietary or scraped data. It contains only code, workflow, and sample datasets for educational purposes.

Workflow Overview

The project follows a classic ETL pipeline with reporting:

[Data Source / Placeholder Data] 
            │
            ▼
       Extract (ETL) 
       - Read raw daily position data
       - Parse and normalize columns
            │
            ▼
     Transform (ETL)
       - Compute net positions (Final_Position)
       - Clean, deduplicate, sort, and format
            │
            ▼
       Load & Report
       - Save to Excel workbook (Sheet1 & Sheet2)
       - Generate charts comparing net positions vs settlement price

Features

Processes daily futures positions from structured datasets or placeholder data.

Computes net positions (Final_Position) from long and short columns.

Compiles historical data into a structured Excel workbook:

Sheet1: full daily data

Sheet2: cleaned and sorted by maximum absolute net position per date

Generates a chart comparing net position vs settlement price using matplotlib.

Handles weekend skipping and incremental updates automatically.
