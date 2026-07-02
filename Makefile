# Local build + deploy for the cryptalytics stack.
# `make local` (the default) builds all four images at service-level context
# and applies the local overlay. Images stay linux/amd64 and land in Docker
# Desktop's image store, which the imagePullPolicy: Never pods read from.

.DEFAULT_GOAL := local
.PHONY: local

PLATFORM := linux/amd64

local:
	docker build --platform $(PLATFORM) -t cryptalytics/ingestion:latest \
	  --build-context pylib=shared/pylib --build-context pb=shared/pb \
	  -f services/ingestion/Dockerfile services/ingestion
	docker build --platform $(PLATFORM) -t cryptalytics/tick-processor:latest \
	  --build-context pylib=shared/pylib \
	  -f services/tick-processor/Dockerfile services/tick-processor
	docker build --platform $(PLATFORM) -t cryptalytics/backfill:latest \
	  --build-context pylib=shared/pylib \
	  -f services/backfill/Dockerfile services/backfill
	docker build --platform $(PLATFORM) -t cryptalytics/airflow:latest \
	  --build-context pb=shared/pb \
	  -f orchestration/Dockerfile orchestration
	kustomize build --enable-helm infrastructure/kubernetes/overlays/local | kubectl apply -f -