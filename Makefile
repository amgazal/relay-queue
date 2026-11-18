.PHONY: build test vet db-up db-down migrate test-schema
PSQL = docker compose exec -T db sh -c 'exec psql -X -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" "$$@"' sh

build:
	go build -o bin/ ./cmd/...

test:
	go test ./...

vet:
	go vet ./...

db-up:
	docker compose up -d --wait db

db-down:
	docker compose down

migrate: db-up
	$(PSQL) -f /migrations/apply.psql

test-schema: migrate
	$(PSQL) < tests/schema.sql
