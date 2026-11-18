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
	docker compose run --rm --no-deps migrate

test-schema: migrate
	$(PSQL) < tests/schema.sql

.PHONY: up down logs images demo
up:
	docker compose up --build -d --wait

down:
	docker compose --profile demo down

logs:
	docker compose logs -f api worker

images:
	docker compose --profile demo build

demo:
	docker compose --profile demo up --build -d --wait

.PHONY: test-integration smoke
test-integration:
	@test -n "$$RELAY_TEST_DATABASE_URL" || (echo "Set RELAY_TEST_DATABASE_URL for PostgreSQL integration tests" >&2; exit 1)
	go test -count=1 -race ./...

smoke:
	python3 tests/compose_smoke.py

.PHONY: bench-chaos bench-chaos-full bench-api bench-scale bench-e2e bench-verify bench-clean
bench-chaos:
	python3 benchmarks/run.py chaos --chaos-jobs 200
bench-chaos-full:
	python3 benchmarks/run.py chaos --chaos-jobs 10000
bench-api:
	python3 benchmarks/run.py api
bench-scale:
	python3 benchmarks/run.py scale
bench-e2e:
	python3 benchmarks/run.py sustained
bench-verify:
	python3 benchmarks/verify.py benchmarks/results.json
bench-clean:
	python3 benchmarks/cleanup.py "$(PROJECT)"
