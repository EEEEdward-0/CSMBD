
# Passenger Flight Data Analysis Tool

## Overview

This Python tool analyzes airline passenger flight data to identify the most frequent flyers.  
It applies a multithreaded MapReduce-style approach to process large datasets efficiently.  
The system outputs summaries, logs, visualizations, and an interactive map of flight routes.

---

## Features

### **Top Passenger Identification**
- Identifies the maximum number of flights taken by any passenger
- Lists all passengers with this highest count

### **Multithreaded MapReduce**
- Map phase splits the dataset into chunks and processes them in parallel
- Combiner used per chunk for local aggregation
- Reduce phase merges partial results into a final count dictionary

### **Flight Route Tracking**
- Records flight numbers and routes for each passenger
- Filters and plots only the top passengers

### **Visualizations**
- **Histogram**: Distribution of flight departures over 24 hours
- **Static 2D Plot**: Top passenger routes plotted as lines on a coordinate map
- **Interactive Map (HTML)**: Flight paths shown on a world map using Plotly

### **Logging & Reporting**
- Logs execution info and issues to `Log/process.log`
- Saves a complete summary of results in JSON format (`Log/summary.json`)

---

## Quick Start

1. Ensure required CSV files are in `data/Task A/`:
   - `AComp_Passenger_data_no_error.csv`
   - `AComp_Passenger_data_no_error_DateTime.csv`
   - `Top30_airports_LatLong.csv`
2. Install dependencies:
   ```bash
   pip install matplotlib seaborn plotly
   ```
3. Run the script:
   ```bash
   python Task\ A\ -\ main.py
   ```

---

## Directory Structure

```
project_root/
├── data/
│   └── Task A/
│       ├── AComp_Passenger_data_no_error.csv
│       ├── AComp_Passenger_data_no_error_DateTime.csv
│       └── Top30_airports_LatLong.csv
├── output/
│   └── Task A/
│       ├── departure_histogram.png
│       ├── flight_routes.png
│       └── flight_map.html
├── Log/
│   ├── process.log
│   └── summary.json
├── Task A - main.py
```

---

## Output

### Console Output
- Number of flights taken by the most frequent passenger(s)
- Passenger ID(s) with the most flights

### Generated Files
- `departure_histogram.png` – Histogram of departure hours (UTC)
- `flight_routes.png` – Static 2D plot of top passenger routes
- `flight_map.html` – Interactive route map using Plotly
- `summary.json` – JSON output summarizing flight counts
- `process.log` – Detailed log of pipeline execution

---

## Version and Changelog

**Current version**: `v2.0`  
**Last updated**: April 25, 2025

### Version History

| Version | Date       | Description                                                                 |
|---------|------------|-----------------------------------------------------------------------------|
| v1.0    | 2025-04-23 | Initial version: functional pipeline with basic visualizations              |
| v2.0    | 2025-04-25 | Refactored into class-based design, added parallel MapReduce, logging, JSON |

