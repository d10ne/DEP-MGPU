FROM python:3.12.3

RUN apt-get update && apt-get install -y libspatialindex-dev libpq-dev build-essential && rm -rf /var/lib/apt/lists/*
WORKDIR /app

RUN pip install --no-cache-dir streamlit pandas psycopg2-binary osmnx networkx folium streamlit-folium
COPY app/routing.py .

ENV STREAMLIT_SERVER_PORT=8501
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0
EXPOSE 8501
ENTRYPOINT ["streamlit", "run", "routing.py"]