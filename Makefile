# airflow-trigger-compare — one command per lifecycle step.
N ?= 20
SCENARIOS ?= a c d

.PHONY: up down unpause seed-% compare logs-notifier clean

up:            ## start the stack (Airflow UI: http://localhost:8080)
	docker compose up --build -d
	@echo "waiting for Airflow health..."
	@until docker compose exec -T airflow curl -fsS \
	  http://localhost:8080/api/v2/monitor/health >/dev/null 2>&1; do sleep 5; done
	$(MAKE) unpause
	@echo "ready."

unpause:       ## unpause all compare DAGs (new DAGs start paused)
	-docker compose exec -T airflow airflow dags unpause worker_per_file
	-docker compose exec -T airflow airflow dags unpause doorbell_expand
	-docker compose exec -T airflow airflow dags unpause controller_fanout
	-docker compose exec -T airflow airflow dags unpause native_pull_asset

seed-%:        ## seed one burst, e.g. make seed-a N=50
	python scripts/seed.py --scenario $* --n $(N)

compare:       ## full run: seed -> bell -> wait -> measure, per scenario
	./scripts/compare.sh "$(SCENARIOS)" $(N)

logs-notifier: ## scenario A trigger activity
	docker compose logs -f notifier

down:
	docker compose down

clean:         ## down + wipe DB volume and reports
	docker compose down -v
	rm -f results/puts_* results/report_* results/compare_*
