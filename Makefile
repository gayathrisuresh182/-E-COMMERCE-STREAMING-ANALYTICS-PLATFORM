.PHONY: infra api dashboard dagster all stop

# Start Kafka, Zookeeper, MongoDB, Kafka UI
infra:
	docker-compose up -d

# Start FastAPI backend
api:
	uvicorn api.main:app --port 8000 --reload

# Start React dashboard (dev mode)
dashboard:
	cd dashboard && npm run dev

# Start Dagster orchestrator
dagster:
	DAGSTER_HOME=$(shell pwd) dagster dev -m ecommerce_analytics

# Start API + Dashboard together
serve: api dashboard

# Stop infrastructure
stop:
	docker-compose down

# Install Python deps
install:
	pip install -e .

# Install dashboard deps
install-dashboard:
	cd dashboard && npm install

# Full setup
setup: install install-dashboard infra
