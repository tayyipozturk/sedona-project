
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.analysis.edge_classifier import classify_edge_type

def test_cases():
    edge1 = {'highway': 'trunk', 'ref': 'D750', 'length': 100}
    edge2 = {'highway': 'secondary', 'ref': '06-30', 'length': 50}
    edge3 = {'highway': 'tertiary', 'ref': '', 'length': 10}

    print("Test 1:", classify_edge_type(edge1))  # Expected: HIGHWAY
    print("Test 2:", classify_edge_type(edge2))  # Expected: MAIN_ROAD
    print("Test 3:", classify_edge_type(edge3))  # Expected: SHORT_SEGMENT

if __name__ == "__main__":
    test_cases()
