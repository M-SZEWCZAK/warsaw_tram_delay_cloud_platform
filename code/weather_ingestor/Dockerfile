FROM python:3.12-slim
WORKDIR /app
COPY weather_ingestor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared/ ./shared/
COPY weather_ingestor/ ./weather_ingestor/
CMD ["python", "-m", "weather_ingestor.main"]
