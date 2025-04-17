# Analysis of Road Network via Apache Sedona/Spark

This project presents a scalable, modular, and spatially-aware analysis system for road networks using **Apache Sedona** and **Apache Spark**. It ingests OpenStreetMap-based road graph data in CSV format and applies various geospatial and topological inferences to derive meaningful metrics from the road infrastructure.

## 🚀 Features

- Spatial joins and queries using Apache Sedona
- Edge-level analytics (e.g., bottleneck detection, heatmaps, speed estimation)
- Node-level analysis (e.g., accessibility, intersection scores)
- Topological inference and spatial cross-analysis
- Modular, testable architecture following SOLID principles

## 📁 Project Structure

```
src/
  analysis/
    edge/                # Edge-based analyses like bottlenecks, density, speed
    node/                # Node-based features: accessibility, intersections
    cross/               # Cross-domain spatial-topological queries
    spatial/             # Spatial join & ops via Sedona
  topological_inference_queries.py  # Inference methods across graph topology
  common/, config/       # Utilities and configurations
tests/                   # Unit tests grouped by spatial domain
main.py                  # Entry point for launching experiments
```

## 📦 Installation

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Apache Spark and Sedona must be properly set up (see `spark-submit` usage).

## 🧪 Running the Project

```bash
spark-submit main.py
```

You can run individual test modules for specific spatial operations:

```bash
pytest tests/
```

## 📊 Sample Output

- Accessibility heatmaps for nodes
- Bottleneck roads per area
- Speed profile clustering for routes
- Intersection density metrics
- Topological inferences: connectedness, critical nodes

## 📚 Methods by Module

### `cross.cross_queries`
- **`find_edges_near_nodes()`**: Identifies edges (roads) that lie near specific nodes (intersections).
- **`anchor_edges_to_nodes()`**: Anchors edges spatially to nodes for topology correction.
- **`estimate_node_road_centrality()`**: Estimates a node's centrality based on surrounding roads.
- **`local_average_speed_per_node()`**: Computes average speed of nearby edges for a node.
- **`classify_intersection_types()`**: Categorizes intersections based on their connecting road count or angles.
- **`assign_traffic_proxy_to_edges()`**: Assigns synthetic traffic load to edges using spatial heuristics.
- **`compute_road_class_accessibility()`**: Evaluates node-level access by road class (e.g., motorway, primary).
- **`estimate_node_to_node_travel()`**: Estimates travel time or cost between nodes using road metadata.
- **`detect_entry_exit_nodes()`**: Detects boundary entry/exit points from the graph.

### `edge.bottleneck_detector`
- **`compute_bottleneck_score()`**: Scores roads based on characteristics like length, width, and potential congestion.

### `edge.density_heatmap`
- **`compute_density_heatmap()`**: Generates a heatmap representing road density across the region.

### `edge.edge_queries`
- **`summarize_roads_by_type()`**: Aggregates road lengths or counts by road type (e.g., residential, primary).
- **`compute_grid_coverage()`**: Computes how much of a spatial grid is covered by roads.
- **`clip_edges_to_bbox()`**: Trims edge geometries to a bounding box for regional analysis.
- **`find_short_segments()`**: Finds unusually short road segments which may indicate data errors or local paths.
- **`summarize_road_types()`**: Summarizes the distribution of road types within the dataset.
- **`estimate_connected_components()`**: Estimates connected subgraphs of the road network.
- **`compute_road_intersections()`**: Calculates where roads intersect and logs intersecting geometries.
- **`summarize_bridges_tunnels()`**: Summarizes bridge and tunnel presence from edge metadata.
- **`top_longest_named_roads()`**: Identifies the longest continuous roads grouped by name.

### `edge.speed_estimator`
- **`compute_speed_estimation()`**: Estimates travel speed across road segments using metadata and topology.

### `match`
- **`match_line_to_road()`**: Matches arbitrary lines (e.g., GPS paths) to the closest road segments.
- **`find_road_from_point()`**: Identifies the road segment closest to a specific point.
- **`find_nearest_intersection()`**: Finds the closest intersection node from a given geometry.
- **`find_connected_intersection_from_line()`**: Finds intersections connected to the endpoints of a line segment.

### `node.accessibility`
- **`compute_accessibility_score()`**: Calculates a node's accessibility based on connected roads and potential reachability.

### `node.dead_end`
- **`compute_dead_end_nodes()`**: Identifies dead-end nodes (cul-de-sacs or terminal intersections) in the network.

### `node.intersection_score`
- **`compute_intersection_score()`**: Assigns a score to intersections based on connectivity and road types.

### `node.node_queries`
- **`find_nearest_node()`**: Finds the closest node to a given point based on spatial distance.
- **`detect_major_intersections()`**: Detects highly connected nodes likely representing key road intersections.
- **`compute_intersection_density()`**: Computes intersection density within a given region to assess urban compactness.
- **`estimate_urban_radius()`**: Estimates the urban center's spatial influence using node density.
- **`intersection_distribution_by_radius()`**: Analyzes how intersections are spatially distributed outward from a center.

### `spatial.spatial_joins`
- **`point_in_polygon_join()`**: Finds points located within specified polygon regions.
- **`line_intersects_polygon_join()`**: Identifies line features that intersect with given polygons.
- **`polygon_overlaps_join()`**: Detects overlapping polygon pairs.
- **`point_distance_join()`**: Performs proximity-based join to link points within a set distance.
- **`range_query_polygon()`**: Retrieves polygons within a query range (bounding box or spatial filter).
- **`linestring_touches_polygon()`**: Finds lines that touch but do not cross polygon boundaries.
- **`point_crosses_linestring()`**: Determines points that intersect lines from one side to another.
- **`indexed_point_in_polygon()`**: Optimized point-in-polygon join using spatial index.
- **`indexed_polygon_polygon_overlap()`**: Efficient polygon overlap detection using spatial index.
- **`safe_partition()`**: Handles spatial partitioning safely for small datasets.

### `spatial.spatial_ops`
- **`spatial_partition_rdd()`**: Partitions spatial data for parallel processing.
- **`spatial_join_points_with_polygon()`**: Joins point datasets with polygons for zonal statistics or tagging.
- **`spatial_join_with_index()`**: Executes spatial join with pre-built index for faster matching.
- **`buffer_and_filter_points()`**: Applies buffer zone around points and filters intersecting features.
- **`filter_points_within_polygon()`**: Filters points that fall within specified polygon boundaries.

### `topological_inference_queries`
- **`estimate_betweenness_proxy()`**: Approximates node importance using shortcut-based betweenness proxy.
- **`detect_cycles()`**: Detects closed loops or cyclic paths within the road network.
- **`detect_subnetworks_by_attribute()`**: Splits the network into subnetworks using attributes (e.g., highway class).
- **`check_directional_conflicts()`**: Checks for conflicting one-way directionality in road topology.
- **`classify_junction_shapes()`**: Classifies junction shapes (e.g., T-junction, roundabout) by geometry.
