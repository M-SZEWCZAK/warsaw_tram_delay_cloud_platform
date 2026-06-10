FROM python:3.12-slim
WORKDIR /app
COPY position_ingestor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared/ ./shared/
COPY position_ingestor/ ./position_ingestor/
CMD ["python", "-m", "position_ingestor.main"]
