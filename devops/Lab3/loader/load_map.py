import osmnx as ox

# настройки
ox.settings.timeout = 600
ox.settings.use_cache = True
ox.settings.overpass_endpoint = "https://overpass.kumi.systems/api/interpreter"

# bbox из Uber данных
north = 40.82
south = 40.70
east = -73.93
west = -74.02

print("Downloading road network...")

G = ox.graph_from_bbox(
    bbox=(north, south, east, west),
    network_type="drive"
)

ox.save_graphml(G, "uber_network.graphml")

print("Graph saved")