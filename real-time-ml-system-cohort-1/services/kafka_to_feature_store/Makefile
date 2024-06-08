build:
	docker build -t kafka_to_feature_store .

run:
	docker run -it --network redpanda_network --env KAFKA_BROKER_ADDRESS=redpanda-0:9092 kafka_to_feature_store

localrun:
	poetry run python src/main.py


lint:
	ruff check --fix

format:
	ruff format .