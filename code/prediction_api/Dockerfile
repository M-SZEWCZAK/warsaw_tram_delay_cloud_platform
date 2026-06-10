FROM python:3.12-slim
WORKDIR /app
COPY prediction_api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared/ ./shared/
COPY prediction_api/ ./prediction_api/
EXPOSE 8080
CMD ["python", "-m", "prediction_api.main"]
