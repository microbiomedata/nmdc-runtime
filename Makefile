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
	COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 \
		docker compose up --build --force-recreate --detach

down-dev:
	docker compose down

docker-image:
	./docker-build.sh polyneme/nmdc-runtime nmdc_runtime/dagster.Dockerfile

terminus-docker-image:
	./docker-build.sh polyneme/terminusdb-server nmdc_runtime/terminus.Dockerfile

publish:
	invoke publish

.PHONY: init update-deps update up-dev down-dev docker-image publish