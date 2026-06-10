"""
shared/bq_schemas.py
BigQuery table schemas.  Import these in any service that writes to BQ so the
schema is defined in one canonical place.
"""
from google.cloud.bigquery import SchemaField

TIMETABLE_SCHEMA = [
    SchemaField("stop_id",              "STRING",    mode="REQUIRED"),
    SchemaField("stop_name",            "STRING",    mode="NULLABLE"),
    SchemaField("stop_nr",              "STRING",    mode="NULLABLE"),  # slupek
    SchemaField("lat",                  "FLOAT64",   mode="NULLABLE"),
    SchemaField("lon",                  "FLOAT64",   mode="NULLABLE"),
    SchemaField("line",                 "STRING",    mode="REQUIRED"),
    SchemaField("brigade",              "STRING",    mode="REQUIRED"),
    SchemaField("scheduled_departure",  "TIMESTAMP", mode="REQUIRED"),
    SchemaField("load_date",            "DATE",      mode="REQUIRED"),  # partition key
]

POSITIONS_RAW_SCHEMA = [
    SchemaField("vehicle_number",  "STRING",    mode="REQUIRED"),
    SchemaField("line",            "STRING",    mode="NULLABLE"),
    SchemaField("brigade",         "STRING",    mode="NULLABLE"),
    SchemaField("lat",             "FLOAT64",   mode="NULLABLE"),
    SchemaField("lon",             "FLOAT64",   mode="NULLABLE"),
    SchemaField("gps_time",        "TIMESTAMP", mode="REQUIRED"),
    SchemaField("ingested_at",     "TIMESTAMP", mode="REQUIRED"),  # partition key
]

POSITIONS_ENRICHED_SCHEMA = [
    SchemaField("vehicle_number",         "STRING",    mode="REQUIRED"),
    SchemaField("line",                   "STRING",    mode="NULLABLE"),
    SchemaField("brigade",                "STRING",    mode="NULLABLE"),
    SchemaField("lat",                    "FLOAT64",   mode="NULLABLE"),
    SchemaField("lon",                    "FLOAT64",   mode="NULLABLE"),
    SchemaField("gps_time",               "TIMESTAMP", mode="REQUIRED"),  # partition key
    SchemaField("ingested_at",            "TIMESTAMP", mode="REQUIRED"),
    SchemaField("matched_stop_id",        "STRING",    mode="NULLABLE"),
    SchemaField("scheduled_departure",    "TIMESTAMP", mode="NULLABLE"),
    SchemaField("delay_s",                "INT64",     mode="NULLABLE"),
    SchemaField("precip_mm",              "FLOAT64",   mode="NULLABLE"),
    SchemaField("temp_c",                 "FLOAT64",   mode="NULLABLE"),
]

FEATURES_SCHEMA = [
    SchemaField("line",                  "STRING",  mode="REQUIRED"),
    SchemaField("brigade",               "STRING",  mode="REQUIRED"),
    SchemaField("stop_id",               "STRING",  mode="REQUIRED"),
    SchemaField("hour_of_day",           "INT64",   mode="REQUIRED"),
    SchemaField("mean_delay_s",          "FLOAT64", mode="NULLABLE"),
    SchemaField("stddev_delay_s",        "FLOAT64", mode="NULLABLE"),
    SchemaField("rain_delay_corr",       "FLOAT64", mode="NULLABLE"),
    SchemaField("peak_hour_flag",        "BOOL",    mode="NULLABLE"),
    SchemaField("sample_count",          "INT64",   mode="NULLABLE"),
    SchemaField("window_start",          "DATE",    mode="REQUIRED"),
    SchemaField("window_end",            "DATE",    mode="REQUIRED"),
    SchemaField("computed_at",           "TIMESTAMP", mode="REQUIRED"),
]
