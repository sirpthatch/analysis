import csv
import heapq
import json
import os
import random
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import geopandas as gpd
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import psutil
import math
from sklearn.cluster import KMeans


@dataclass(frozen=True)
class Coordinate:
    latitude: float
    longitude: float

    def __str__(self):
        return f"({self.latitude:.6f}, {self.longitude:.6f})"
    
    def distance_to(self, other: 'Coordinate') -> float:
        """
        Returns the distance to another coordinate, measured in feet.

        Uses the Haversine formula to calculate great-circle distance between
        two points on a sphere given their longitudes and latitudes.

        Args:
            other: Another Coordinate object

        Returns:
            Distance in feet
        """
        import math

        # Earth's radius in feet (mean radius)
        EARTH_RADIUS_FEET = 20_902_231  # approximately 3,959 miles * 5,280 feet/mile

        # Convert degrees to radians
        lat1_rad = math.radians(self.latitude)
        lat2_rad = math.radians(other.latitude)
        lon1_rad = math.radians(self.longitude)
        lon2_rad = math.radians(other.longitude)

        # Haversine formula
        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad

        a = (math.sin(dlat / 2) ** 2 +
             math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2)
        c = 2 * math.asin(math.sqrt(a))

        # Calculate distance in feet
        distance = EARTH_RADIUS_FEET * c

        return distance

@dataclass
class StepMetric:
    """Metrics for a single execution step"""
    step_name: str
    elapsed_time: float  # seconds
    memory_mb: float
    timestamp: float  # Unix timestamp

class RouteStep:
    """A step in a route between two coordinates"""

    def __init__(self, coor_a: Coordinate, coor_b: Coordinate):
        self.coor_a = coor_a
        self.coor_b = coor_b
        self._distance: Optional[float] = None 

    @property
    def distance(self) -> float:
        """Lazy-loaded distance property - only calculated when first accessed"""
        if self._distance is None:
            self._distance = self.coor_a.distance_to(self.coor_b)
        return self._distance

@dataclass
class Route:
    start:Coordinate
    end:Coordinate
    steps:List[RouteStep]

    def total_distance(self) -> float:
        return sum([s.distance for s in self.steps])

class PathFinderAlgo(object):

    def __init__(self, location_path: Path):
        self.locations = []
        with open(location_path) as handle:
            reader = csv.DictReader(handle)
            for line in reader:
                self.locations.append(Coordinate(float(line["Latitude"]),
                                                 float(line["Longitude"])))

    def _get_borough(self, coord: Coordinate) -> str:
        """
        Determine which NYC borough a coordinate is in based on approximate boundaries.

        Staten Island: roughly lat 40.49-40.65, lon -74.26 to -74.05
        Brooklyn: roughly lat 40.57-40.74, lon -74.04 to -73.83
        Manhattan: roughly lat 40.70-40.88, lon -74.02 to -73.91
        Queens: roughly lat 40.54-40.80, lon -73.96 to -73.70
        Bronx: roughly lat 40.79-40.92, lon -73.93 to -73.75
        """
        lat, lon = coord.latitude, coord.longitude

        # Staten Island (most isolated, check first)
        if 40.49 <= lat <= 40.65 and -74.26 <= lon <= -74.05:
            return "Staten Island"

        # Manhattan (most central, narrow longitude range)
        if 40.70 <= lat <= 40.88 and -74.02 <= lon <= -73.91:
            return "Manhattan"

        # Bronx (northernmost)
        if 40.79 <= lat <= 40.92 and -73.93 <= lon <= -73.75:
            return "Bronx"

        # Brooklyn (southern, western)
        if 40.57 <= lat <= 40.74 and -74.04 <= lon <= -73.83:
            return "Brooklyn"

        # Queens (eastern)
        if 40.54 <= lat <= 40.80 and -73.96 <= lon <= -73.70:
            return "Queens"

        # Default to closest match by latitude
        if lat < 40.65:
            return "Staten Island" if lon < -74.05 else "Brooklyn"
        elif lat > 40.82:
            return "Bronx"
        else:
            return "Manhattan" if lon > -74.0 else "Brooklyn"

    def calculate_distances(self) -> Dict[Coordinate, List[Tuple[float, Coordinate]]]:
        """
        Returns a Map[Coordinate -> List[(Distance, Coordinate)]]

        Each coordinate maps to a sorted list of (distance, coordinate) tuples,
        sorted by distance (nearest first).

        Returns:
            Dictionary mapping each coordinate to a list of (distance, coordinate)
            tuples sorted by distance
        """
        distance_map = {}

        for coord_a in self.locations:
            # Build priority queue of distances from coord_a to all others
            distances = []

            for coord_b in self.locations:
                if coord_a != coord_b:
                    distance = coord_a.distance_to(coord_b)
                    distances.append((distance, coord_b))

            # Sort by distance (heapq maintains min-heap property)
            heapq.heapify(distances)
            distance_map[coord_a] = distances

        return distance_map
    
    def calculate_shortest_path(self, starting_point: Coordinate,
                                distances: Dict[Coordinate, List[Tuple[float, Coordinate]]]) -> Route:
        """
        Returns a path that traverses all of the points in distances map, attempting to minimize
        overall distance using a greedy nearest-neighbor heuristic.

        This implements an approximation to the Traveling Salesman Problem (TSP) with a
        borough constraint: once entering Staten Island, the route must visit all Staten Island
        locations before moving to another borough.

        The algorithm:
        1. Start at the given starting point
        2. Always move to the nearest unvisited location
        3. If current location is in Staten Island, only consider Staten Island neighbors
           until all Staten Island locations are visited
        4. Continue until all locations are visited

        Args:
            starting_point: The coordinate to start the route from
            distances: Pre-calculated distance matrix

        Returns:
            A Route object containing all locations visited exactly once
        """
        # Track visited locations
        visited = set()
        visited.add(starting_point)

        # Identify all Staten Island locations
        staten_island_coords = {
            coord for coord in self.locations
            if self._get_borough(coord) == "Staten Island"
        }

        # Track whether we've entered Staten Island
        in_staten_island = (starting_point in staten_island_coords)
        staten_island_complete = False

        # Build the route steps
        steps = []
        current = starting_point

        # Visit all locations using nearest neighbor heuristic with Staten Island constraint
        while len(visited) < len(self.locations):
            # Get sorted distances for current location
            nearest_neighbors = distances[current]

            # Determine if we're currently in Staten Island
            current_borough = self._get_borough(current)
            in_staten_island = (current_borough == "Staten Island")

            # Check if all Staten Island locations have been visited
            unvisited_staten_island = staten_island_coords - visited
            staten_island_complete = (len(unvisited_staten_island) == 0)

            # Find nearest unvisited neighbor
            next_location = None
            for dist, coord in nearest_neighbors:
                if coord not in visited:
                    coord_borough = self._get_borough(coord)

                    # If in Staten Island and not all SI locations visited,
                    # only consider Staten Island locations
                    if in_staten_island and not staten_island_complete:
                        if coord_borough == "Staten Island":
                            next_location = coord
                            break
                    else:
                        # Otherwise, take nearest unvisited location
                        next_location = coord
                        break

            
            if next_location is None:
                # This shouldn't happen if distances map is complete
                break
            """
                print(f"[WARNING] No unvisited neighbor found from {current}")
                print(f"  Current borough: {current_borough}")
                print(f"  In Staten Island: {in_staten_island}")
                print(f"  Staten Island complete: {staten_island_complete}")
                print(f"  Unvisited SI locations: {len(unvisited_staten_island)}")
                break
            """

            # Create step and add to route
            step = RouteStep(current, next_location)
            steps.append(step)

            # Mark as visited and move to next location
            visited.add(next_location)
            current = next_location

        # Create the route
        # End point is the last location visited
        end_point = current

        route = Route(
            start=starting_point,
            end=end_point,
            steps=steps
        )

        return route
    
    def pick_starting_point(self) -> Coordinate:
        return Coordinate(40.587520, -73.795700) # Saved from previous optimization
        # return random.choice(self.locations)
    
    def find_best_starting_point(self) -> Coordinate:
        """
        Iterates over all coordinates to find the best one to start on
        (based on minimizing the total distance).

        This method tries every location as a potential starting point,
        calculates the resulting route, and returns the coordinate that
        produces the shortest total route distance.

        Returns:
            The coordinate that produces the shortest route
        """
        print(f"[OPTIMIZATION] Finding best starting point from {len(self.locations)} locations...")

        # Pre-calculate distance matrix once
        distances = self.calculate_distances()

        best_start = None
        best_distance = float('inf')

        # Try each location as a starting point
        for i, start_coord in enumerate(self.locations):
            # Calculate route from this starting point
            route = self.calculate_shortest_path(start_coord, distances)
            total_dist = route.total_distance()

            # Track the best route found so far
            if total_dist < best_distance:
                best_distance = total_dist
                best_start = start_coord

            # Progress update every 10% of locations
            if (i + 1) % max(1, len(self.locations) // 10) == 0:
                progress = (i + 1) / len(self.locations) * 100
                print(f"  Progress: {progress:.0f}% ({i+1}/{len(self.locations)}) "
                      f"- Best so far: {best_distance / 5280:.2f} miles")

        print(f"[OPTIMIZATION] Best starting point found: {best_start}")
        print(f"[OPTIMIZATION] Best route distance: {best_distance / 5280:.2f} miles")

        return best_start

    def check_route(self, route: Route) -> bool:
        """
        Verifies that the route is 'good'.

        A route is good if it contains all of the locations
        once and only once, and no additional locations

        Args:
            route: The route to validate

        Returns:
            True if the route is valid, False otherwise
        """
        is_valid = True

        # Collect all coordinates from the route steps
        visited_coords = set()
        all_route_coords = []

        # Extract coordinates from each step
        for i, step in enumerate(route.steps):
            if i == 0:
                # First step: include both start and end
                all_route_coords.append(step.coor_a)
                all_route_coords.append(step.coor_b)
            else:
                # Subsequent steps: only include the destination
                all_route_coords.append(step.coor_b)

        # Check for duplicates
        for coord in all_route_coords:
            if coord in visited_coords:
                print(f"[ROUTE ERROR] Duplicate coordinate found in route: {coord}")
                is_valid = False
            visited_coords.add(coord)

        # Check if route contains all locations
        expected_locations = set(self.locations)

        # Check for missing locations
        missing_locations = expected_locations - visited_coords
        if missing_locations:
            print(f"[ROUTE ERROR] Route is missing {len(missing_locations)} location(s):")
            for coord in list(missing_locations)[:5]:  # Show first 5
                print(f"  - {coord}")
            if len(missing_locations) > 5:
                print(f"  ... and {len(missing_locations) - 5} more")
            is_valid = False

        # Check for extra locations not in the original set
        extra_locations = visited_coords - expected_locations
        if extra_locations:
            print(f"[ROUTE ERROR] Route contains {len(extra_locations)} extra location(s) not in original set:")
            for coord in list(extra_locations)[:5]:  # Show first 5
                print(f"  - {coord}")
            if len(extra_locations) > 5:
                print(f"  ... and {len(extra_locations) - 5} more")
            is_valid = False

        # Check route continuity (each step's end matches next step's start)
        for i in range(len(route.steps) - 1):
            current_end = route.steps[i].coor_b
            next_start = route.steps[i + 1].coor_a
            if current_end != next_start:
                print(f"[ROUTE ERROR] Route discontinuity at step {i}->{i+1}:")
                print(f"  Step {i} ends at:   {current_end}")
                print(f"  Step {i+1} starts at: {next_start}")
                is_valid = False

        # Check start and end coordinates match route.start and route.end
        if route.steps:
            if route.steps[0].coor_a != route.start:
                print(f"[ROUTE ERROR] Route start mismatch:")
                print(f"  Expected: {route.start}")
                print(f"  Actual:   {route.steps[0].coor_a}")
                is_valid = False

            if route.steps[-1].coor_b != route.end:
                print(f"[ROUTE ERROR] Route end mismatch:")
                print(f"  Expected: {route.end}")
                print(f"  Actual:   {route.steps[-1].coor_b}")
                is_valid = False
        else:
            print(f"[ROUTE ERROR] Route has no steps")
            is_valid = False

        # Log success if valid
        if is_valid:
            print(f"[ROUTE OK] Route is valid:")
            print(f"  - Contains all {len(self.locations)} locations")
            print(f"  - No duplicates")
            print(f"  - No extra locations")
            print(f"  - Continuous path")
            print(f"  - Total distance: {route.total_distance():,.2f} feet")

        return is_valid

class PathRenderer(object):

    def __init__(self, render_boroughs = True):
        self.render_boroughs = render_boroughs

    def render_route(self, route: Route, output_dir: Path) -> str:
        """
        Renders an image of the route and stores it in the
        supplied path directory.

        The image will render lines for all of the edges on the path in
        ascending rainbow colors, with NYC borough boundaries.

        Args:
            route: The route to render
            output_dir: Directory to save the rendered image

        Returns:
            Path to the saved image file
        """
        # Create output directory if it doesn't exist
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"route.png"

        # Create figure
        fig, ax = plt.subplots(figsize=(16, 12))

        if self.render_boroughs:
            # Load NYC borough boundaries
            borough_shapefile = Path(__file__).parent.parent / "data" / "ext_data" / "nybb_25c" / "nybb.shp"
            try:
                boroughs = gpd.read_file(borough_shapefile)

                # Reproject from State Plane (NAD 1983 FIPS 3104 feet) to WGS84 (lat/lon)
                # The shapefile uses EPSG:2263 (NAD83 / New York Long Island)
                if boroughs.crs is None:
                    print(f"[WARNING] Borough shapefile has no CRS, assuming EPSG:2263")
                    boroughs = boroughs.set_crs("EPSG:2263")

                # Convert to WGS84 (EPSG:4326) for lat/lon coordinates
                boroughs = boroughs.to_crs("EPSG:4326")

                has_boroughs = True
                print(f"✓ Loaded {len(boroughs)} NYC borough boundaries")
                print(f"  Reprojected from {boroughs.crs.name if boroughs.crs else 'unknown'} to WGS84")
            except Exception as e:
                print(f"[WARNING] Could not load borough boundaries: {e}")
                has_boroughs = False

            # Draw borough boundaries first (as background)
            if has_boroughs:
                boroughs.boundary.plot(ax=ax, linewidth=2.5, edgecolor='lightgray', alpha=0.6, zorder=0)

        # Extract coordinates from route steps
        lats = []
        lons = []
        for i, step in enumerate(route.steps):
            if i == 0:
                # First step: add both start and end
                lats.append(step.coor_a.latitude)
                lons.append(step.coor_a.longitude)
            lats.append(step.coor_b.latitude)
            lons.append(step.coor_b.longitude)

        # Generate rainbow colors for edges
        num_steps = len(route.steps)
        colors = plt.cm.rainbow(np.linspace(0, 1, num_steps))

        # Draw edges with rainbow colors
        for i, step in enumerate(route.steps):
            ax.plot(
                [step.coor_a.longitude, step.coor_b.longitude],
                [step.coor_a.latitude, step.coor_b.latitude],
                color=colors[i],
                linewidth=1.5,
                alpha=0.7,
                zorder=2
            )

        # Plot all points
        ax.scatter(lons, lats, c='black', s=20, alpha=0.6, zorder=3, label='Locations')

        # Highlight start and end points
        ax.scatter(
            [route.start.longitude],
            [route.start.latitude],
            c='green',
            s=200,
            marker='o',
            edgecolors='black',
            linewidth=2,
            zorder=4,
            label='Start'
        )
        ax.scatter(
            [route.end.longitude],
            [route.end.latitude],
            c='red',
            s=200,
            marker='s',
            edgecolors='black',
            linewidth=2,
            zorder=4,
            label='End'
        )
        

        # Add labels and title
        ax.set_xlabel('Longitude', fontsize=12, fontweight='bold')
        ax.set_ylabel('Latitude', fontsize=12, fontweight='bold')
        ax.set_title(
            f'Route Visualization of NYC Toilet Run\n{len(route.steps)} steps, '
            f'{route.total_distance() / 5280:.2f} miles total',
            fontsize=14,
            fontweight='bold'
        )
        ax.legend(loc='best', fontsize=10, framealpha=0.9)
        ax.grid(True, alpha=0.3)

        # Set aspect ratio to be equal
        ax.set_aspect('equal', adjustable='box')

        # Add colorbar to show progression
        sm = plt.cm.ScalarMappable(
            cmap=plt.cm.rainbow,
            norm=plt.Normalize(vmin=0, vmax=num_steps)
        )
        sm.set_array([])
        plt.colorbar(sm, ax=ax, label='Step Number', shrink=0.8)

        # Save figure
        plt.tight_layout()
        plt.savefig(output_file, dpi=150, bbox_inches='tight')
        plt.close(fig)

        print(f"✓ Route visualization saved to: {output_file}")

        return str(output_file)


class ClusteredPathFinderAlgo(PathFinderAlgo):

    def pick_starting_point(self):
        return Coordinate(40.573670, -73.992700) # from optimization run with 20 clusters

    def optimize_parameters(self, max_clusters=20) -> Tuple[int, Coordinate]:
        """
        Iterates cluster sizes from 0..max_clusters and all of the possible start points
        to find the combination of start point and cluster size that creates the shortest route

        Returns:
            The coordinate that produces the shortest route
        """
        print(f"[OPTIMIZATION] Finding best starting point and number of clusters point from {len(self.locations)} locations...")

        # Pre-calculate distance matrix once
        distances = self.calculate_distances()

        best_start = None
        best_distance = float('inf')
        best_cluster_size = None
        total_search_space = max(1, len(self.locations)) * max_clusters

        num_steps = 0
        for n in range(1, max_clusters):
            # Try each location as a starting point
            for i, start_coord in enumerate(self.locations):
                # Calculate route from this starting point
                route = self.calculate_shortest_path(start_coord, distances, {"num_clusters":n})
                total_dist = route.total_distance()

                # Track the best route found so far
                if total_dist < best_distance:
                    best_distance = total_dist
                    best_start = start_coord
                    best_cluster_size = n

                # Progress update every 5% of progress
                num_steps += 1
                if num_steps % 5 == 0:
                    progress = (num_steps / total_search_space*1.0) * 100
                    print(f"  Progress: {progress:.0f}% ({num_steps}/{total_search_space}) "
                        f"- Best so far: {best_distance / 5280:.2f} miles")

        print(f"[OPTIMIZATION] Best starting point found: {best_start}")
        print(f"[OPTIMIZATION] Best cluster size: {best_cluster_size} clusters")
        print(f"[OPTIMIZATION] Best route distance: {best_distance / 5280:.2f} miles")

        return best_start


    def _find_nearest_point(self, 
                            point:Coordinate, 
                            points:List[Coordinate], 
                            visited:set[Coordinate], 
                            distances) -> Coordinate:
        # Get sorted distances for current location
        nearest_neighbors = [x for x in distances[point] if x[1] in points]
        # Find nearest unvisited neighbor
        next_location = None
        for dist, coord in nearest_neighbors:
            if coord not in visited:
                next_location = coord
                break
        return next_location

    def calculate_shortest_path(self, starting_point, distances, params={"num_clusters":20}) -> Route:
        """
        Returns a path that traverses all points using clustering to group nearby locations.

        This algorithm:
        1. Uses KMeans to cluster locations into groups
        2. Traverses clusters intelligently based on nearest cluster centroid
        3. Within each cluster, uses nearest neighbor heuristic
        4. Maintains Staten Island invariant: once entering Staten Island, visits all SI locations
           before leaving to another borough

        Args:
            starting_point: The coordinate to start the route from
            distances: Pre-calculated distance matrix
            params: Dictionary with optional 'num_clusters' parameter

        Returns:
            A Route object containing all locations visited exactly once
        """
        num_clusters = 5
        if "num_clusters" in params:
            num_clusters = params["num_clusters"]

        # Identify Staten Island locations
        staten_island_coords = {
            coord for coord in self.locations
            if self._get_borough(coord) == "Staten Island"
        }

        # Prepare data for clustering: convert coordinates to numpy array
        coord_array = np.array([[c.latitude, c.longitude] for c in self.locations])

        # Perform KMeans clustering
        kmeans = KMeans(n_clusters=min(num_clusters, len(self.locations)), random_state=42)
        cluster_labels = kmeans.fit_predict(coord_array)

        # Build cluster mapping: cluster_id -> list of coordinates
        clusters = {}
        for coord, label in zip(self.locations, cluster_labels):
            if label not in clusters:
                clusters[label] = []
            clusters[label].append(coord)

        # Track visited locations and route steps
        visited = set()
        visited.add(starting_point)
        steps = []
        current = starting_point

        # Determine which cluster the starting point belongs to
        current_cluster_id = cluster_labels[self.locations.index(starting_point)]
        visited_clusters = {current_cluster_id}

        # Helper function to find nearest unvisited point within a set of candidates
        def find_nearest_unvisited(from_point, candidates):
            """Find the nearest unvisited point from candidates"""
            nearest_neighbors = distances[from_point]
            for dist, coord in nearest_neighbors:
                if coord in candidates and coord not in visited:
                    return coord
            return None

        # Helper function to find nearest cluster centroid
        def find_nearest_cluster(from_point, available_cluster_ids):
            """Find the cluster with nearest centroid to from_point"""
            if not available_cluster_ids:
                return None

            min_dist = float('inf')
            nearest_cluster = None

            for cluster_id in available_cluster_ids:
                # Calculate distance to cluster centroid
                centroid = kmeans.cluster_centers_[cluster_id]
                centroid_coord = Coordinate(centroid[0], centroid[1])
                dist = from_point.distance_to(centroid_coord)

                if dist < min_dist:
                    min_dist = dist
                    nearest_cluster = cluster_id

            return nearest_cluster

        # Main traversal loop
        while len(visited) < len(self.locations):
            current_borough = self._get_borough(current)
            in_staten_island = (current_borough == "Staten Island")
            unvisited_staten_island = staten_island_coords - visited
            staten_island_complete = (len(unvisited_staten_island) == 0)

            # Determine candidate points for next move
            if in_staten_island and not staten_island_complete:
                # STATEN ISLAND INVARIANT: Must complete all SI locations before leaving
                candidates = unvisited_staten_island
            else:
                # Find unvisited locations in current cluster first
                current_cluster_unvisited = [
                    coord for coord in clusters[current_cluster_id]
                    if coord not in visited
                ]

                if current_cluster_unvisited:
                    # Stay in current cluster
                    candidates = set(current_cluster_unvisited)
                else:
                    # Current cluster complete, move to nearest unvisited cluster
                    unvisited_cluster_ids = set()
                    for cluster_id, coords in clusters.items():
                        if any(coord not in visited for coord in coords):
                            unvisited_cluster_ids.add(cluster_id)

                    if unvisited_cluster_ids:
                        # Find nearest cluster
                        current_cluster_id = find_nearest_cluster(current, unvisited_cluster_ids)
                        visited_clusters.add(current_cluster_id)
                        candidates = set([c for c in clusters[current_cluster_id] if c not in visited])
                    else:
                        # No more clusters (shouldn't happen)
                        candidates = set([c for c in self.locations if c not in visited])

            # Find nearest unvisited point from candidates
            next_location = find_nearest_unvisited(current, candidates)

            if next_location is None:
                # Fallback: find any unvisited location
                next_location = find_nearest_unvisited(current, set(self.locations) - visited)

            if next_location is None:
                # Should not happen if distances map is complete
                break

            # Create step and add to route
            step = RouteStep(current, next_location)
            steps.append(step)

            # Update state
            visited.add(next_location)
            current = next_location

            # Update current cluster if we moved to a different one
            if next_location in self.locations:
                current_cluster_id = cluster_labels[self.locations.index(next_location)]

        # Create and return the route
        route = Route(
            start=starting_point,
            end=current,
            steps=steps
        )

        return route


class PathFinderHarness(object):

    def __init__(self, location_path: Path, output_path:Path):
        self.location_path = location_path
        self.output_path = output_path
        self.process = psutil.Process(os.getpid())
        self.metrics: List[StepMetric] = []
        #self.algo = PathFinderAlgo(self.location_path)

        start_time = time.time()
        #self.algo = PathFinderAlgo(self.location_path)
        self.algo = ClusteredPathFinderAlgo(self.location_path)
        elapsed = time.time() - start_time
        memory_mb = self._get_memory_usage_mb()
        self._emit_step_metrics("Load locations", elapsed, memory_mb)


    def _get_memory_usage_mb(self) -> float:
        """Get current memory usage in MB"""
        return self.process.memory_info().rss / 1024 / 1024

    def _emit_step_metrics(self, step_name: str, elapsed_time: float, memory_mb: float) -> None:
        """Emit timing and memory metrics for a step and store them"""
        # Create metric object
        metric = StepMetric(
            step_name=step_name,
            elapsed_time=elapsed_time,
            memory_mb=memory_mb,
            timestamp=time.time()
        )

        # Store metric
        self.metrics.append(metric)

        # Print metric
        print(f"[METRICS] {step_name}")
        print(f"  ├─ Time:   {elapsed_time:.3f} seconds")
        print(f"  └─ Memory: {memory_mb:.2f} MB")

    def get_metrics(self) -> List[StepMetric]:
        """Return collected metrics"""
        return self.metrics

    def write_metrics_to_file(self, output_path: Path) -> None:
        """Write metrics to a CSV file"""
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w', newline='') as f:
            writer = csv.writer(f)
            # Write header
            writer.writerow(['step_name', 'elapsed_time_seconds', 'memory_mb', 'timestamp'])

            # Write metrics
            for metric in self.metrics:
                writer.writerow([
                    metric.step_name,
                    f"{metric.elapsed_time:.6f}",
                    f"{metric.memory_mb:.2f}",
                    f"{metric.timestamp:.6f}"
                ])

        print(f"\n✓ Metrics written to: {output_path}")

    def optimize(self) -> None:
        self.algo.optimize_parameters()

    def optimize_starting_point(self) -> Route:
        """
        Run the path finder algorithm to find the optimal starting point.

        This method tries all possible starting points and returns the route
        with the shortest total distance. Includes timing and memory diagnostics.

        Returns:
            The optimal route found
        """
        print(f"\nLoaded {len(self.algo.locations)} locations")

        # Step 2: Calculate distance matrix
        start_time = time.time()
        distances = self.algo.calculate_distances()
        elapsed = time.time() - start_time
        memory_mb = self._get_memory_usage_mb()
        self._emit_step_metrics("Calculate distances", elapsed, memory_mb)

        print(f"\nCalculated distances for {len(distances)} coordinates")

        # Step 3: Find optimal starting point (this is the key difference)
        start_time = time.time()
        best_start = self.algo.find_best_starting_point()
        elapsed = time.time() - start_time
        memory_mb = self._get_memory_usage_mb()
        self._emit_step_metrics("Find optimal start", elapsed, memory_mb)

        print(f"\nFound optimal starting point: {best_start}")

        # Step 4: Calculate final route with optimal starting point
        start_time = time.time()
        route = self.algo.calculate_shortest_path(best_start, distances)
        elapsed = time.time() - start_time
        memory_mb = self._get_memory_usage_mb()
        self._emit_step_metrics("Calculate final route", elapsed, memory_mb)

        print(f"\nCalculated optimized route with {len(route.steps)} steps")

        # Step 5: Check Route
        start_time = time.time()
        is_valid = self.algo.check_route(route)
        elapsed = time.time() - start_time
        memory_mb = self._get_memory_usage_mb()
        self._emit_step_metrics("Validated route", elapsed, memory_mb)

        print(f"\nValidated route, is valid: {is_valid}")

        # Step 6: Render Route
        start_time = time.time()
        rendered_path = PathRenderer().render_route(route, self.output_path)
        elapsed = time.time() - start_time
        memory_mb = self._get_memory_usage_mb()
        self._emit_step_metrics("Rendered route", elapsed, memory_mb)

        print(f"\nRoute rendered, saved to: {rendered_path}")

        self.write_metrics_to_file(self.output_path / "diags.csv")
        with open(self.output_path / "route.json","w") as w_handle:
            json.dump({
                "start": asdict(route.start),
                "end": asdict(route.end),
                "distance": route.total_distance() / 5280.0,
                "optimized": True
            }, w_handle, indent=2)

        print("\nFinished (Optimized):")
        print(f"+ Starting at {route.start}")
        print(f"+ Making {len(route.steps)} stops")
        print(f"+ In total, {round(route.total_distance() / 5280.0,2)} miles")
        print(f"+ Finishing at {route.end}")

        # Summary
        print(f"\n{'='*50}")
        print(f"Optimized path finder completed successfully")
        print(f"{'='*50}")

        return route

    def run(self) -> Route:
        """Run the path finder algorithm with timing and memory diagnostics"""

          # Step 2: Calculate distance matrix
        start_time = time.time()
        distances = self.algo.calculate_distances()
        elapsed = time.time() - start_time
        memory_mb = self._get_memory_usage_mb()
        self._emit_step_metrics("Calculate distances", elapsed, memory_mb)

        print(f"\nCalculated distances for {len(distances)} coordinates")
        
        # Step 3: Calculate route
        start_time = time.time()
        route = self.algo.calculate_shortest_path(self.algo.pick_starting_point(), distances)
        elapsed = time.time() - start_time
        memory_mb = self._get_memory_usage_mb()
        self._emit_step_metrics("Calculate route", elapsed, memory_mb)

        print(f"\nCalculated route with {len(route.steps)} steps")

        # Step 4: Check Route
        start_time = time.time()
        is_valid = self.algo.check_route(route)
        elapsed = time.time() - start_time
        memory_mb = self._get_memory_usage_mb()
        self._emit_step_metrics("Validated route", elapsed, memory_mb)

        print(f"\nValidated route, is valid: {is_valid}")

        # Step 5: Render Route
        start_time = time.time()
        rendered_path = PathRenderer().render_route(route, self.output_path)
        elapsed = time.time() - start_time
        memory_mb = self._get_memory_usage_mb()
        self._emit_step_metrics("Rendered route", elapsed, memory_mb)

        print(f"\nRoute rendered, saved to: {rendered_path}")
              
        self.write_metrics_to_file(self.output_path / "diags.csv")
        with open(self.output_path / "route.json","w") as w_handle:
            json.dump({
                "start": asdict(route.start),
                "end": asdict(route.end),
                "distance": route.total_distance() / 5280.0
            }, w_handle, indent=2)

        print("\nFinished:")
        print(f"+ Starting at {route.start}")
        print(f"+ Making {len(route.steps)} stops")
        print(f"+ In total, {round(route.total_distance() / 5280.0,2)} miles")
        print(f"+ Finishing at {route.end}")
        
        # Summary
        print(f"\n{'='*50}")
        print(f"Path finder completed successfully")
        print(f"{'='*50}")

