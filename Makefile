init:
	pip install --upgrade pip-tools pip setuptools
	pip install --editable .
	pip install --upgrade -r requirements/main.txt  -r requirements/dev.txt

update-deps:
	pip install --upgrade pip-tools pip setuptools
	pip-compile --upgrade --build-isolation --generate-hashes --output-file \
		requirements/main.txt requirements/main.in
	pip-compile --upgrade --build-isolation --generate-hashes --output-file \
		requirements/dev.txt requirements/dev.in

update: update-deps init

up-dev:
	docker compose up --build --force-recreate --detach

down-dev:
	docker compose down

fastapi-docker:
	./docker-build.sh polyneme/nmdc-runtime-fastapi nmdc_runtime/fastapi.Dockerfile

dagster-docker:
	./docker-build.sh polyneme/nmdc-runtime-dagster nmdc_runtime/dagster.Dockerfile

publish:
	invoke publish

.PHONY: init update-deps update up-dev down-dev fastapi-docker dagster-docker terminus-docker publish