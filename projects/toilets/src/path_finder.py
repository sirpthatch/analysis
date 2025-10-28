from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Dict, Tuple
import csv
import heapq
import time
import psutil
import os
import random

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

        This implements an approximation to the Traveling Salesman Problem (TSP).
        The algorithm:
        1. Start at the given starting point
        2. Always move to the nearest unvisited location
        3. Continue until all locations are visited
        4. Return to starting point

        Args:
            starting_point: The coordinate to start the route from
            distances: Pre-calculated distance matrix

        Returns:
            A Route object containing all locations visited exactly once
        """
        # Track visited locations
        visited = set()
        visited.add(starting_point)

        # Build the route steps
        steps = []
        current = starting_point

        # Visit all locations using nearest neighbor heuristic
        while len(visited) < len(self.locations):
            # Get sorted distances for current location
            nearest_neighbors = distances[current]

            # Find nearest unvisited neighbor
            next_location = None
            for dist, coord in nearest_neighbors:
                if coord not in visited:
                    next_location = coord
                    break

            if next_location is None:
                # This shouldn't happen if distances map is complete
                print(f"[WARNING] No unvisited neighbor found from {current}")
                break

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
        return random.choice(self.locations)

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

    
class PathFinderHarness(object):

    def __init__(self, location_path: Path):
        self.location_path = location_path
        self.process = psutil.Process(os.getpid())
        self.metrics: List[StepMetric] = []

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

    def run(self) -> Route:
        """Run the path finder algorithm with timing and memory diagnostics"""

        # Step 1: Initialize algorithm and load data
        start_time = time.time()
        algo = PathFinderAlgo(self.location_path)
        elapsed = time.time() - start_time
        memory_mb = self._get_memory_usage_mb()
        self._emit_step_metrics("Load locations", elapsed, memory_mb)

        print(f"\nLoaded {len(algo.locations)} locations")

        # Step 2: Calculate distance matrix
        start_time = time.time()
        distances = algo.calculate_distances()
        elapsed = time.time() - start_time
        memory_mb = self._get_memory_usage_mb()
        self._emit_step_metrics("Calculate distances", elapsed, memory_mb)

        print(f"\nCalculated distances for {len(distances)} coordinates")
        
        # Step 3: Calculate route
        start_time = time.time()
        route = algo.calculate_shortest_path(algo.pick_starting_point(), distances)
        elapsed = time.time() - start_time
        memory_mb = self._get_memory_usage_mb()
        self._emit_step_metrics("Calculate route", elapsed, memory_mb)

        print(f"\nCalculated route with {len(route.steps)} steps")

        # Step 4: Check Route
        start_time = time.time()
        is_valid = algo.check_route(route)
        elapsed = time.time() - start_time
        memory_mb = self._get_memory_usage_mb()
        self._emit_step_metrics("Validated route", elapsed, memory_mb)

        print(f"\nValidated route, is valid: {is_valid}")

        print(f"\n\nRoute distance: {route.total_distance() / 5280.0} miles")
        
        # Summary
        print(f"\n{'='*50}")
        print(f"Path finder completed successfully")
        print(f"{'='*50}")

