.PHONY: up down restart logs test clean migrate help

help:
	@echo "Kasparro Backend & ETL System - Available Commands:"
	@echo "  make up        - Start all services (API + ETL + Database)"
	@echo "  make down      - Stop all services"
	@echo "  make restart   - Restart all services"
	@echo "  make logs      - View logs from all services"
	@echo "  make test      - Run test suite"
	@echo "  make migrate   - Run database migrations"
	@echo "  make clean     - Clean up containers and volumes"
	@echo "  make shell     - Open shell in API container"

up:
	@echo "Starting Kasparro Backend & ETL System..."
	docker-compose up -d --build
	@echo "Waiting for services to be ready..."
	@sleep 5
	@echo "Services are running!"
	@echo "API: http://localhost:8000"
	@echo "Health: http://localhost:8000/health"
	@echo "Docs: http://localhost:8000/docs"

down:
	@echo "Stopping all services..."
	docker-compose down

restart:
	@echo "Restarting services..."
	docker-compose restart

logs:
	docker-compose logs -f

logs-api:
	docker-compose logs -f api

logs-etl:
	docker-compose logs -f etl

test:
	@echo "Running test suite..."
	docker-compose exec api pytest tests/ -v --cov=. --cov-report=term-missing

test-local:
	@echo "Running tests locally..."
	pytest tests/ -v --cov=. --cov-report=html

migrate:
	@echo "Running database migrations..."
	docker-compose exec postgres psql -U kasparro -d kasparro_db -f /docker-entrypoint-initdb.d/init.sql

clean:
	@echo "Cleaning up containers and volumes..."
	docker-compose down -v
	@echo "Removing __pycache__ directories..."
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@echo "Cleanup complete!"

shell:
	docker-compose exec api /bin/bash

db-shell:
	docker-compose exec postgres psql -U kasparro -d kasparro_db

build:
	docker-compose build

status:
	docker-compose ps
