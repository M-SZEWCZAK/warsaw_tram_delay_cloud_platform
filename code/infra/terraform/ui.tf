# infra/terraform/ui.tf
# Deploys the frontend (nginx + index.html) to Cloud Run independently.
# Run: terraform apply -target=google_cloud_run_v2_service.tram_frontend
#
# Prerequisites:
#   1. main.tf has already been applied (Artifact Registry must exist)
#   2. Docker image has been pushed:
#      docker build -t tram-frontend .
#      docker tag tram-frontend \
#        europe-central2-docker.pkg.dev/YOUR_PROJECT_ID/tram-platform/tram-frontend:latest
#      docker push \
#        europe-central2-docker.pkg.dev/YOUR_PROJECT_ID/tram-platform/tram-frontend:latest

# ── Variables (override on CLI or in terraform.tfvars) ────────────────────────

variable "frontend_image_tag" {
  type        = string
  default     = "latest"
  description = "Image tag to deploy, e.g. 'latest' or a specific git SHA"
}

# ── Cloud Run Service ─────────────────────────────────────────────────────────

resource "google_cloud_run_v2_service" "tram_frontend" {
  name     = "tram-frontend"
  location = var.region   # reuses region from main.tf

  template {
    scaling {
      min_instance_count = 0
      max_instance_count = 3
    }
    containers {
      image = "${local.image_prefix}/tram-frontend:${var.frontend_image_tag}"
      ports { container_port = 8080 }
      resources {
        limits = { cpu = "1", memory = "256Mi" }
      }
    }
  }
}

# ── Allow unauthenticated access (public website) ─────────────────────────────

resource "google_cloud_run_v2_service_iam_member" "tram_frontend_public" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.tram_frontend.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

# ── Output the URL ────────────────────────────────────────────────────────────

output "frontend_url" {
  description = "Public URL of the frontend"
  value       = google_cloud_run_v2_service.tram_frontend.uri
}
