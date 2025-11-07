ENV_FILE=.airflow.env

DC=docker compose --env-file $(ENV_FILE)

build:
	$(DC) build

up:
	$(DC) up -d

down:
	$(DC) down -v

restart:
	$(DC) down -v
	$(DC) up -d

postgres:
	docker exec -it airflow-postgres-1 psql -U airflow



