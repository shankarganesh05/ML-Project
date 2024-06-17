SHELL := /bin/bash
build:
	docker build -t trade-producer .

run:
	docker run -it --network redpanda_network --env KAFKA_BROKER_ADDRESS=redpanda-0:9092 trade-producer

lint:
	ruff check --fix
format:
	ruff format .

localrun:
	source live.env && poetry run python src/main.py

localrun_historical:
	source historical.env && poetry run python src/main.py