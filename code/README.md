# Warsaw Tram Delay Prediction Platform

Microservice codebase matching the architecture described in the Milestone 1 report.

## Project Structure

```
warsaw-tram-platform/
├── shared/                     # Shared config, BQ schemas, helpers
├── timetable_loader/           # Cloud Run Job — runs 03:00 daily
├── position_ingestor/          # Cloud Run Job — polls every 60 s
├── weather_ingestor/           # Cloud Run Job — polls every 10 min
├── stream_processor/           # Dataflow streaming pipeline
├── feature_builder/            # BigQuery scheduled SQL query
├── prediction_api/             # Cloud Run service — REST API
└── infra/
    ├── terraform/              # GCP infrastructure as code
    └── scheduler/              # Cloud Scheduler job definitions
```

## Environment Variables

Set once and referenced everywhere:

| Variable | Description |
|---|---|
| `WARSAW_API_KEY` | API key from api.um.warszawa.pl |
| `GCP_PROJECT_ID` | Google Cloud project ID |
| `GCP_REGION` | e.g. `europe-central2` |
| `BQ_DATASET` | BigQuery dataset, default `trams_warsaw` |
| `PUBSUB_POSITIONS_TOPIC` | e.g. `tram-positions` |
| `PUBSUB_WEATHER_TOPIC` | e.g. `tram-weather` |
| `FIRESTORE_COLLECTION` | e.g. `vehicles` |
| `GCS_ARCHIVE_BUCKET` | e.g. `trams-raw-archive` |
| `GCS_ML_BUCKET` | e.g. `trams-ml-artefacts` |
| `PREDICTION_API_ENDPOINT` | Vertex AI endpoint URL |

## Deployment Order

1. `terraform apply` — creates all GCP resources
2. Build & push each service Docker image to Artifact Registry
3. Deploy Cloud Run services / jobs
4. Cloud Scheduler picks up the job definitions automatically
