build:
	docker build -t trade_to_ohlc .

run:
	docker run -it --network redpanda_network --env KAFKA_BROKER_ADDRESS=redpanda-0:9092 trade_to_ohlc

localrun:
	poetry run python src/main.py


lint:
	ruff check --fix

format:
	ruff format .