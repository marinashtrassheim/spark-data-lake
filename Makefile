.PHONY: help up down generate bronze silver gold run notebook clean

SPARK_MASTER := spark://spark-master:7077
SPARK_SUBMIT := docker exec spark-master spark-submit --master $(SPARK_MASTER)
COMPOSE_TOOLS := docker compose --profile tools

help:
	@echo "Available commands:"
	@echo "  make up         - Start MinIO, Spark, and Jupyter (docker compose up -d --build)"
	@echo "  make down       - Stop containers"
	@echo "  make generate   - Generate JSON and write to MinIO raw/ (Docker)"
	@echo "  make bronze     - Run bronze layer"
	@echo "  make silver     - Run silver layer"
	@echo "  make gold       - Run gold layer"
	@echo "  make run        - Full pipeline (generate → bronze → silver → gold)"
	@echo "  make notebook   - Jupyter at http://localhost:8889 (run after make run)"
	@echo "  make clean      - Stop containers and remove Docker volumes"

up:
	docker compose up -d --build

down:
	docker compose down

generate:
	$(COMPOSE_TOOLS) run --rm --build generator

bronze:
	$(SPARK_SUBMIT) /opt/spark-apps/bronze_ingest.py

silver:
	$(SPARK_SUBMIT) /opt/spark-apps/silver_merge.py

gold:
	$(SPARK_SUBMIT) /opt/spark-apps/gold_aggregate.py

run: generate bronze silver gold

notebook:
	docker compose up -d --build jupyter
	@echo "Jupyter (no login): http://localhost:8889/tree"
	@echo "Open notebooks/explore_medallion_lake.ipynb (run cells top to bottom after make run)"

clean:
	docker compose down -v
