# Passenger Flight Data Analysis Tool

## Overview

This Python script performs an analysis of passenger flight data. It identifies the passenger(s) who have taken the most flights, visualizes flight departure time distributions, and plots the geographical routes taken by the most frequent flyers.

## Features

* **Frequent Flyer Identification**: Determines the maximum number of flights taken by any single passenger and lists the ID(s) of passenger(s) achieving this maximum.
* **Detailed Flight Logs**: Prints a formatted table detailing the flights (Flight ID, Origin Airport, Destination Airport) for each top passenger.
* **Departure Time Analysis**: Generates a histogram visualizing the distribution of flight departure hours (UTC) across all flights.
* **Route Visualization (Static)**: Creates a static plot showing the flight routes (origin to destination lines) for the top passenger(s) on a 2D latitude/longitude graph.
* **Route Visualization (Interactive)**: Generates an interactive HTML map using Plotly, displaying the flight routes of the top passenger(s) on a world map.

## Requirements

* Python 3.x
* Libraries:
    * `pandas`
    * `matplotlib`
    * `seaborn`
    * `plotly`

You can install the required libraries using pip:

```bash
pip install pandas matplotlib seaborn plotly
The script also uses standard Python libraries: csv, datetime, os, and collections.

Input Data Files
The script requires the following CSV files to be present at the specified paths:

AComp_Passenger_data_no_error.csv:

Location: Defined by PATH_CLEAN.
Expected Format: CSV file where columns include at least: Flight ID (index 0), Passenger ID (index 1), Origin Airport Code (index 2), and Destination Airport Code (index 3). Used for identifying top passengers and their routes.
Example Row Snippet: FL123,PAX456,LHR,JFK,...
AComp_Passenger_data_no_error_DateTime.csv:

Location: Defined by PATH_DATETIME.
Expected Format: CSV file where column index 4 contains the departure time as a Unix timestamp (integer seconds since the epoch). Used for the departure time histogram.
Example Row Snippet: ...,...,...,...,1678886400,... (representing a timestamp)
Top30_airports_LatLong.csv:

Location: Defined by PATH_AIRPORTS.
Expected Format: CSV file containing airport data. Relevant columns are Airport Code (index 1), Latitude (index 2), and Longitude (index 3). Used for mapping airport codes to geographical coordinates for plotting routes.
Example Row Snippet: ...,LHR,51.4700,-0.4543,...
Configuration
Before running, ensure the following path variables at the beginning of the script point to the correct locations of your input files and desired output directory:

Python

# File paths
PATH_CLEAN    = "/path/to/your/AComp_Passenger_data_no_error.csv"
PATH_DATETIME = "/path/to/your/AComp_Passenger_data_no_error_DateTime.csv"
PATH_AIRPORTS = "/path/to/your/Top30_airports_LatLong.csv"
OUTPUT_DIR    = "/path/to/your/output/directory"
Usage
Configure the PATH_CLEAN, PATH_DATETIME, PATH_AIRPORTS, and OUTPUT_DIR variables in the script.

Make sure the required Python libraries are installed.

Run the script from your terminal:

Bash

python your_script_name.py
(Replace your_script_name.py with the actual filename).

The script will automatically create the OUTPUT_DIR if it doesn't exist.

Output
The script produces the following outputs:

Console Output:

The maximum number of flights recorded for a single passenger.
The Passenger ID(s) of the top frequent flyer(s).
For each top passenger, a table listing their Flight ID, Origin ('From'), and Destination ('To') for every flight they took.
Generated Files (saved in the specified OUTPUT_DIR):

departure_histogram.png: A histogram showing the number of flights departing in each hour of the day (0-23 UTC).
flight_routes.png: A static plot visualizing the flight routes (lines connecting origin and destination airports) for the top passenger(s).
flight_map.html: An interactive HTML file that displays the flight routes of the top passenger(s) on a geographical map. You can open this file in a web browser to pan, zoom, and hover over points.