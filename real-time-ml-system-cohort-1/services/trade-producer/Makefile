build:
	docker build -t trade-producer .

run:
	docker run -it --network redpanda_network trade-producer

lint:
	ruff check --fix
format:
	ruff format .
