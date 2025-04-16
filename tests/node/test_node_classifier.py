
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.analysis.node_classifier import classify_node_type

def test_cases():
    node1 = {'street_count': 1, 'highway': 'turning_loop', 'junction': ''}
    node2 = {'street_count': 3, 'highway': '', 'junction': 'yes'}
    node3 = {'street_count': 4, 'highway': 'turning_circle', 'junction': ''}

    print("Test 1:", classify_node_type(node1))  # Expected: TURNING_LOOP
    print("Test 2:", classify_node_type(node2))  # Expected: INTERSECTION or CROSSROAD
    print("Test 3:", classify_node_type(node3))  # Expected: ROUNDABOUT

if __name__ == "__main__":
    test_cases()
