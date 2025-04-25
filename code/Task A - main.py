import csv
import datetime
import os
import json
import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go

# Define project directories
BASE_DIR   = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
DATA_DIR   = os.path.join(BASE_DIR, "data", "Task A")        # CSV inputs live here
OUTPUT_DIR = os.path.join(BASE_DIR, "output", "Task A")      # PNG/HTML outputs saved here
LOG_DIR    = os.path.join(BASE_DIR, "Log")                   # logs and JSON summary here

# Input CSV file paths
PATH_CLEAN    = os.path.join(DATA_DIR, "AComp_Passenger_data_no_error.csv")
PATH_DATETIME = os.path.join(DATA_DIR, "AComp_Passenger_data_no_error_DateTime.csv")
PATH_AIRPORTS = os.path.join(DATA_DIR, "Top30_airports_LatLong.csv")

# Ensure output & log directories exist
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# Configure file-based logging
log_path = os.path.join(LOG_DIR, "process.log")
logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger("FlightAnalyzer")

class FlightAnalyzer:
    def __init__(self, clean_path, datetime_path, airports_path):
        # Store file paths
        self.clean_path    = clean_path
        self.datetime_path = datetime_path
        self.airports_path = airports_path
        # Prepare containers for MapReduce
        self.partial_counts = []             # combiner outputs per chunk
        self.flights        = defaultdict(list)  # passenger -> flight_nos
        self.routes         = defaultdict(list)  # passenger -> list of (from,to)
        self.counts         = {}             # final reduced counts

    def map_row(self, row):
        """
        Parse a CSV row into (passenger_id, 1, flight_no, (from, to)).
        Logs and skips malformed rows.
        """
        try:
            pid       = row[1].strip()    # passenger ID field
            flight_no = row[0].strip()    # flight number field
            frm       = row[2].strip()    # origin airport code
            to        = row[3].strip()    # destination airport code
            return pid, 1, flight_no, (frm, to)
        except Exception as e:
            logger.warning(f"Failed to map row {row}: {e}")
            return None

    def _process_chunk(self, chunk):
        """
        Map+Combiner on a chunk of rows:
        - Map each row
        - Locally aggregate counts per passenger in this chunk
        - Record flight numbers and routes
        Returns a dict passenger -> local count
        """
        partial = defaultdict(int)
        for row in chunk:
            if len(row) < 4:
                continue
            mapped = self.map_row(row)
            if mapped:
                pid, one, flight_no, route = mapped
                partial[pid] += one            # combiner step
                self.flights[pid].append(flight_no)
                self.routes[pid].append(route)
        return partial

    def map_phase(self, max_workers=4):
        """
        Perform the Map phase in parallel:
        - Read all rows, skip header
        - Split into chunks
        - Execute _process_chunk concurrently
        - Collect combiner outputs
        """
        with open(self.clean_path, newline='', encoding='utf-8') as f:
            rows = list(csv.reader(f))[1:]  # skip header row
        size = max(1, len(rows) // max_workers)
        chunks = [rows[i:i+size] for i in range(0, len(rows), size)]

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self._process_chunk, chunk) for chunk in chunks]
            for fut in as_completed(futures):
                partial = fut.result()
                self.partial_counts.append(partial)
                logger.info(f"Chunk combiner produced {sum(partial.values())} flights")

    def reduce_phase(self):
        """
        Shuffle+Reduce:
        - Merge all partial_counts into final counts
        """
        self.counts = defaultdict(int)
        for partial in self.partial_counts:
            for pid, cnt in partial.items():
                self.counts[pid] += cnt
        logger.info("Reduce phase completed")

    def find_top(self):
        """
        Identify the maximum flight count and the passenger(s) who have it.
        """
        max_count = max(self.counts.values())
        tops = [pid for pid, cnt in self.counts.items() if cnt == max_count]
        logger.info(f"Top count={max_count}, passengers={tops}")
        return max_count, tops

    def export_summary(self, max_count, tops):
        """
        Write a JSON summary of results to Log/summary.json
        """
        summary = {
            "max_flights": max_count,
            "top_passengers": tops,
            "counts": self.counts
        }
        path = os.path.join(LOG_DIR, "summary.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
        logger.info(f"Summary JSON saved to {path}")

    def get_hours(self):
        """
        Extract departure hours (0–23) from DateTime CSV
        """
        hours = []
        with open(self.datetime_path, newline='', encoding='utf-8') as f:
            for row in csv.reader(f):
                try:
                    ts = int(row[4])
                    hours.append(datetime.datetime.utcfromtimestamp(ts).hour)
                except:
                    continue
        return hours

    def plot_histogram(self):
        """
        Generate and save a histogram of departure hours to PNG
        """
        hours = self.get_hours()
        plt.figure(figsize=(10,5))
        sns.histplot(hours, bins=24, edgecolor='black')
        plt.title("Departure Hour Distribution")
        plt.xlabel("Hour (0–23)")
        plt.ylabel("Flight Count")
        plt.grid(alpha=0.3)
        path = os.path.join(OUTPUT_DIR, "departure_histogram.png")
        plt.savefig(path, dpi=300)
        plt.close()
        logger.info(f"Histogram saved to {path}")

    def load_coords(self):
        """
        Load airport lat/lon coordinates from CSV into a dict
        """
        coords = {}
        with open(self.airports_path, newline='', encoding='utf-8') as f:
            for row in csv.reader(f):
                if len(row) < 4:
                    continue
                code = row[1].strip()
                try:
                    coords[code] = (float(row[2]), float(row[3]))
                except:
                    continue
        return coords

    def plot_routes(self, coords, tops):
        """
        Plot static 2D routes for top passengers to PNG
        """
        plt.figure(figsize=(8,6))
        for pid in tops:
            for frm, to in self.routes[pid]:
                if frm in coords and to in coords and frm != to:
                    lat1, lon1 = coords[frm]
                    lat2, lon2 = coords[to]
                    plt.plot([lon1, lon2], [lat1, lat2], marker='o', alpha=0.6)
        plt.title("Top Passenger Flight Routes")
        plt.xlabel("Longitude")
        plt.ylabel("Latitude")
        plt.grid(alpha=0.3)
        path = os.path.join(OUTPUT_DIR, "flight_routes.png")
        plt.savefig(path, dpi=300)
        plt.close()
        logger.info(f"Routes plot saved to {path}")

    def draw_map(self, coords, tops):
        """
        Generate interactive flight map in HTML using Plotly
        """
        fig = go.Figure()
        for pid in tops:
            for frm, to in self.routes[pid]:
                if frm in coords and to in coords and frm != to:
                    lat1, lon1 = coords[frm]
                    lat2, lon2 = coords[to]
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
                landcolor='rgb(240,240,240)',
                countrycolor='rgb(200,200,200)'
            )
        )
        path = os.path.join(OUTPUT_DIR, "flight_map.html")
        fig.write_html(path)
        logger.info(f"Interactive map saved to {path}")

    def run(self):
        """
        Execute full MapReduce pipeline and generate outputs:
        1. map_phase (with combiner)
        2. reduce_phase (shuffle+reduce)
        3. find_top
        4. export_summary
        5. plot_histogram
        6. load_coords + plot_routes + draw_map
        """
        self.map_phase(max_workers=4)
        self.reduce_phase()
        max_count, tops = self.find_top()
        self.export_summary(max_count, tops)
        self.plot_histogram()
        coords = self.load_coords()
        self.plot_routes(coords, tops)
        self.draw_map(coords, tops)
        return max_count, tops

if __name__ == "__main__":
    analyzer = FlightAnalyzer(PATH_CLEAN, PATH_DATETIME, PATH_AIRPORTS)
    max_flights, top_passengers = analyzer.run()
    print(f"Max flights: {max_flights}")
    print(f"Top passenger ID(s): {top_passengers}")
