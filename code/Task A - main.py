import csv
import datetime
import os
import pandas as pd
from collections import defaultdict
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go

# Base directories
# BASE_DIR: project root (one level above this script)
# DATA_DIR: location of Task A CSV files
# OUTPUT_DIR: directory for generated outputs
BASE_DIR    = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
DATA_DIR    = os.path.join(BASE_DIR, "data", "Task A")
OUTPUT_DIR  = os.path.join(BASE_DIR, "output", "Task A")

# File paths for input data
PATH_CLEAN    = os.path.join(DATA_DIR, "AComp_Passenger_data_no_error.csv")
PATH_DATETIME = os.path.join(DATA_DIR, "AComp_Passenger_data_no_error_DateTime.csv")
PATH_AIRPORTS = os.path.join(DATA_DIR, "Top30_airports_LatLong.csv")

def map_line_to_passenger(line):
    """
    Mapper: extract passenger ID from a CSV row.
    Returns (passenger_id, 1) or None on error.
    """
    try:
        passenger_id = line[1].strip()
        return passenger_id, 1
    except IndexError:
        return None

def reduce_passenger_counts(mapped_results):
    """
    Reducer: aggregate flight counts per passenger ID.
    """
    counts = defaultdict(int)
    for item in mapped_results:
        if item:
            pid, count = item
            counts[pid] += count
    return counts

def find_top_passengers(counts):
    """
    Identify the maximum flight count and the list of passenger IDs
    who have that count.
    """
    max_count = max(counts.values())
    top_ids = [pid for pid, c in counts.items() if c == max_count]
    return max_count, top_ids

def extract_departure_hours(datetime_path):
    """
    Read the timestamp field from the DateTime CSV and convert to hour.
    Returns a list of integer hours (0–23).
    """
    hours = []
    with open(datetime_path, newline='', encoding='utf-8') as f:
        for row in csv.reader(f):
            try:
                ts = int(row[4])
                dt = datetime.datetime.utcfromtimestamp(ts)
                hours.append(dt.hour)
            except:
                continue
    return hours

def plot_departure_histogram(datetime_path):
    """
    Plot and save a histogram of departure hours.
    """
    hours = extract_departure_hours(datetime_path)
    plt.figure(figsize=(10, 5))
    sns.histplot(hours, bins=24, edgecolor='black')
    plt.title("Departure Hour Distribution")
    plt.xlabel("Hour (0–23)")
    plt.ylabel("Flight Count")
    plt.grid(alpha=0.3)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    plt.savefig(os.path.join(OUTPUT_DIR, "departure_histogram.png"), dpi=300)
    plt.close()

def load_airport_coords(airports_path):
    """
    Load airport IATA codes and their latitude/longitude coordinates.
    Returns a dict code -> (lat, lon).
    """
    coords = {}
    with open(airports_path, newline='', encoding='utf-8') as f:
        for row in csv.reader(f):
            if len(row) < 4:
                continue
            code = row[1].strip()
            try:
                lat = float(row[2])
                lon = float(row[3])
            except:
                continue
            coords[code] = (lat, lon)
    return coords

def plot_routes(clean_path, coords, top_passengers):
    """
    Plot and save a 2D overview of flight routes for top passengers.
    """
    routes = []
    with open(clean_path, newline='', encoding='utf-8') as f:
        for row in csv.reader(f):
            if len(row) < 4:
                continue
            pid, frm, to = row[1].strip(), row[2].strip(), row[3].strip()
            if pid in top_passengers and frm != to and frm in coords and to in coords:
                routes.append((coords[frm], coords[to]))
    plt.figure(figsize=(8, 6))
    for (lat1, lon1), (lat2, lon2) in routes:
        plt.plot([lon1, lon2], [lat1, lat2], marker='o', alpha=0.6)
    plt.title("Top Passenger Flight Routes")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.grid(alpha=0.3)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    plt.savefig(os.path.join(OUTPUT_DIR, "flight_routes.png"), dpi=300)
    plt.close()

def draw_flight_map(clean_path, coords, top_passengers):
    """
    Generate and save an interactive HTML map of flight routes.
    """
    routes = []
    with open(clean_path, newline='', encoding='utf-8') as f:
        for row in csv.reader(f):
            if len(row) < 4:
                continue
            pid, frm, to = row[1].strip(), row[2].strip(), row[3].strip()
            if pid in top_passengers and frm != to and frm in coords and to in coords:
                routes.append((coords[frm], coords[to]))
    fig = go.Figure()
    for (lat1, lon1), (lat2, lon2) in routes:
        fig.add_trace(go.Scattergeo(
            lon=[lon1, lon2],
            lat=[lat1, lat2],
            mode='lines+markers',
            line=dict(width=2, color='royalblue'),
            marker=dict(size=4, color='red'),
            opacity=0.8
        ))
    fig.update_layout(
        title='Top Passenger Flight Map',
        showlegend=False,
        geo=dict(
            projection_type='natural earth',
            showland=True,
            landcolor='rgb(240, 240, 240)',
            countrycolor='rgb(200, 200, 200)'
        )
    )
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    fig.write_html(os.path.join(OUTPUT_DIR, "flight_map.html"))

def main():
    """
    Main pipeline:
      1. MapReduce to find top passenger(s)
      2. Print summary and flight details
      3. Generate and save visualizations
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Map step: extract (passenger_id, 1), and collect flights/routes
    mapped_results = []
    flights = defaultdict(list)
    routes_data = defaultdict(list)
    with open(PATH_CLEAN, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader, None)  # skip header if present
        for row in reader:
            if len(row) < 4:
                continue
            pid = row[1].strip()
            flight_no = row[0].strip()
            frm, to = row[2].strip(), row[3].strip()
            mapped_results.append(map_line_to_passenger(row))
            flights[pid].append(flight_no)
            routes_data[pid].append((frm, to))

    # Reduce step: aggregate counts and find top
    counts = reduce_passenger_counts(mapped_results)
    max_flights, top_passengers = find_top_passengers(counts)

    # Output results to console
    print(f"Max flights: {max_flights}")
    print(f"Top passenger ID(s): {top_passengers}\n")
    for pid in top_passengers:
        flight_list = ", ".join(flights[pid])
        route_list = ", ".join(f"{frm}->{to}" for frm, to in routes_data[pid])
        print(f"Passenger {pid} flights: {flight_list}")
        print(f"Passenger {pid} routes: {route_list}\n")

    # Generate visualizations
    plot_departure_histogram(PATH_DATETIME)
    coords = load_airport_coords(PATH_AIRPORTS)
    plot_routes(PATH_CLEAN, coords, top_passengers)
    draw_flight_map(PATH_CLEAN, coords, top_passengers)

if __name__ == "__main__":
    main()
