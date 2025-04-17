from analysis.match import find_connected_intersection_from_line, find_nearest_intersection, find_road_from_point, match_line_to_road

def test_match_line_to_road(sedona, edges_df, tolerance=0.0005):
    test_lines = [
        "LINESTRING (33.5393424 38.937902, 33.5387301 38.9388888)", # E90 Karayolu Blv. D750 Converge
        "LINESTRING (33.5391104 38.9379491, 33.5400000 38.9390000)", # E90 Karayolu Blv. D750 Exact
        "LINESTRING (33.4343223 39.0526529, 33.4350000 39.0530000)", # Ankara-Adana D750 Converge
        "LINESTRING (33.4340000 39.0520000, 33.4350000 39.0530000)", # Ankara-Adana D750 Converge more
        "LINESTRING (33.5 39.0, 33.6 39.1)" # Higher tolerance, should not match
    ]

    for i, wkt in enumerate(test_lines, 1):
        print(f"\n=== Test {i}: {wkt} ===")
        result = match_line_to_road(sedona, edges_df, wkt, tolerance)
        result.show(truncate=False)


def test_match_point_to_road(sedona, edges_df, tolerance=0.0005):
    point_wkts = [
        "POINT (33.58 38.98)",
        "POINT (33.4343223 39.0526529)"
    ]

    for i, wkt in enumerate(point_wkts, 1):
        print(f"\n=== Test {i}: Find Road From Point {wkt} ===")
        find_road_from_point(sedona, edges_df, wkt, tolerance).show(truncate=False)


def test_find_nearest_intersection_to_point(sedona, nodes_df, tolerance=0.0005):
    point_wkts = [
        "POINT (33.52 38.92)",
        "POINT (33.53 38.93)",
        "POINT (33.5393420 38.937900)",
        "POINT (33.5393424 38.937902)", # Exact match
        "POINT (33.4343223 39.0526529)"
    ]
    
    for i, wkt in enumerate(point_wkts, 1):
        print(f"\n=== Test {i}: Find Nearest Intersection From Point {wkt} ===")
        find_nearest_intersection(sedona, nodes_df, wkt, tolerance).show(truncate=False)


def test_find_connected_intersection_from_line(sedona, nodes_df, tolerance=0.0005):
    line_wkts = [
        "LINESTRING (33.51 38.92, 33.01 38.93)", # No intersection
        "LINESTRING (33.53934 38.9379, 33.53873 38.93888)", # Close
        "LINESTRING (33.539342 38.93790, 33.538730 38.938888)", # Closer
        "LINESTRING (33.539342 38.93790, 38.9388888 33.5387301 )", # One point is close, other is reversed
        "LINESTRING (33.5393424 38.937902, 33.5387301 38.9388888)", # Exact match
        "LINESTRING (33.4343223 39.0526529, 33.4350000 39.0530000)" # Exact match
    ]
    
    for i, wkt in enumerate(line_wkts, 1):
        print(f"\n=== Test {i}: Find Connected Intersection From Line {wkt} ===")
        find_connected_intersection_from_line(sedona, nodes_df, wkt, tolerance).show(truncate=False)