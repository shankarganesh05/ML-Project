start-redpanda:
	docker compose -f redpanda.yml up -d
stop-redpanda:
	docker compose -f redpanda.yml down

build-technical_indicator_pipeline:
	docker compose -f technical_indicator_pipeline.yml build
	
start-technical_indicator_pipeline: build-technical_indicator_pipeline
	docker compose -f technical_indicator_pipeline.yml up -d

stop-technical_indicator_pipeline:
	docker compose -f technical_indicator_pipeline.yml down

build-backfill-pipeline:
	docker compose -f backfill_pipeline.yml build

start-backfill-pipeline: build-backfill-pipeline
	docker compose -f backfill_pipeline.yml up -d

stop-backfill-pipeline:
	docker compose -f backfill_pipeline.yml down

