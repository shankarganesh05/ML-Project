start-redpanda:
	docker compose -f redpanda.yml up -d
stop-redpanda:
	docker compose -f redpanda.yml down -d

build-feature_pipeline:
	docker compose -f feature_pipeline.yml build
	
start-feature_pipeline: build-feature_pipeline
	docker compose -f feature_pipeline.yml up -d

stop-feature_pipeline:
	docker compose -f feature_pipeline.yml down
