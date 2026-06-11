FROM python:3.12-slim
WORKDIR /app
COPY training_job/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared/ ./shared/
COPY training_job/ ./training_job/
CMD ["python", "-m", "training_job.train"]
