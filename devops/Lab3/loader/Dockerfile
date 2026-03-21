FROM python:3.12.3
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY loader.py /app/loader.py
CMD ["python", "/app/loader.py"]