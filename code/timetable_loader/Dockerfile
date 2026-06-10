FROM python:3.12-slim

WORKDIR /app
COPY timetable_loader/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ ./shared/
COPY timetable_loader/ ./timetable_loader/

CMD ["python", "-m", "timetable_loader.main"]
