FROM apache/beam_python3.12_sdk:2.57.0
WORKDIR /app
COPY shared/ ./shared/
COPY stream_processor/ ./stream_processor/
RUN pip install --no-cache-dir google-cloud-firestore==2.16.0 google-cloud-bigquery==3.22.0
ENV PYTHONPATH=/app