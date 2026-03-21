import streamlit as st
import pandas as pd
import psycopg2
import osmnx as ox
import networkx as nx
import folium
import os
from streamlit_folium import st_folium

st.set_page_config(layout="wide")

st.title("🚕 Uber Trips Routing Dashboard")

# -----------------------------------
# Подключение к БД
# -----------------------------------
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

conn = get_connection()

# -----------------------------------
# Загрузка поездок
# -----------------------------------
@st.cache_data
def load_data():
    query = """
        SELECT id, lat, lon
        FROM trips
        ORDER BY random()
        LIMIT 200
    """
    return pd.read_sql(query, conn)

df = load_data()

st.subheader("Uber Trips Sample")
st.dataframe(df)

# -----------------------------------
# Выбор поездок
# -----------------------------------
col1, col2 = st.columns(2)

with col1:
    start_id = st.selectbox("Start trip", df["id"])

with col2:
    end_id = st.selectbox("End trip", df["id"])

start = df[df["id"] == start_id].iloc[0]
end = df[df["id"] == end_id].iloc[0]

# -----------------------------------
# Загрузка дорожного графа
# -----------------------------------
@st.cache_resource
def load_graph():
    return ox.load_graphml("/data/uber_network.graphml")

G = load_graph()

# -----------------------------------
# Поиск ближайших узлов
# -----------------------------------
orig_node = ox.distance.nearest_nodes(G, start.lon, start.lat)
dest_node = ox.distance.nearest_nodes(G, end.lon, end.lat)

# -----------------------------------
# Кратчайший маршрут
# -----------------------------------
route = nx.shortest_path(
    G,
    orig_node,
    dest_node,
    weight="length"
)

# -----------------------------------
# Длина маршрута
# -----------------------------------
length = nx.shortest_path_length(
    G,
    orig_node,
    dest_node,
    weight="length"
)

distance_km = length / 1000

# средняя скорость Uber
speed_kmh = 30
time_minutes = (distance_km / speed_kmh) * 60

# -----------------------------------
# Метрики
# -----------------------------------
col1, col2 = st.columns(2)

col1.metric("Distance", f"{distance_km:.2f} km")
col2.metric("Estimated Time", f"{time_minutes:.1f} min")

# -----------------------------------
# Карта
# -----------------------------------
st.subheader("Route Map")

# центр карты
center_lat = (start.lat + end.lat) / 2
center_lon = (start.lon + end.lon) / 2

m = folium.Map(location=[center_lat, center_lon], zoom_start=12)

# преобразуем маршрут в GeoDataFrame
route_gdf = ox.routing.route_to_gdf(G, route)

# координаты линии
coords = []
for geom in route_gdf.geometry:
    xs, ys = geom.xy
    for x, y in zip(xs, ys):
        coords.append((y, x))

# линия маршрута
folium.PolyLine(
    coords,
    color="red",
    weight=6
).add_to(m)

# старт
folium.Marker(
    [start.lat, start.lon],
    popup="Start",
    icon=folium.Icon(color="green")
).add_to(m)

# конец
folium.Marker(
    [end.lat, end.lon],
    popup="End",
    icon=folium.Icon(color="red")
).add_to(m)

# показать все поездки
for _, row in df.iterrows():
    folium.CircleMarker(
        [row.lat, row.lon],
        radius=2,
        color="blue",
        fill=True
    ).add_to(m)

st_folium(m, width=1000, height=600)