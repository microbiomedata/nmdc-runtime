init:
	pip install --upgrade pip-tools pip setuptools
	pip install --editable .
	pip install --upgrade -r requirements/main.txt  -r requirements/dev.txt

update-deps:
	pip install --upgrade pip-tools pip setuptools
	pip-compile --upgrade --build-isolation --output-file \
		requirements/main.txt requirements/main.in
	pip-compile --upgrade --build-isolation --output-file \
		requirements/dev.txt requirements/dev.in

update: update-deps init

update-schema:
	pip-compile --upgrade-package nmdc-schema --build-isolation --output-file \
		requirements/main.txt requirements/main.in
	pip install -r requirements/main.txt

update-schema-yaml:
	# XXX depends on git checkout of nmdc-schema repo in sibling directory.
	# XXX you likely want to `git checkout vX.Y.Z`, where nmdc-schema==X.Y.Z is installed
	rm -rf nmdc_schema_yaml_src
	cp -r ../nmdc-schema/src/schema/ nmdc_schema_yaml_src

up-dev:
	docker compose up --build --force-recreate --detach --remove-orphans

up-test:
	docker compose --file docker-compose.test.yml \
		up --build --force-recreate --detach

test-build:
	docker compose --file docker-compose.test.yml \
		up test --build --force-recreate --detach

test-dbinit:
	docker compose --file docker-compose.test.yml \
		run --entrypoint ./tests/mongorestore-nmdc-testdb.sh test

test-run:
	docker compose --file docker-compose.test.yml run test

test: test-build test-dbinit test-run

lint:
	# Python syntax errors or undefined names
	flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics --extend-ignore=F722
	# exit-zero treats all errors as warnings. The GitHub editor is 127 chars wide
	flake8 . --count --exit-zero --max-complexity=10 --max-line-length=127 \
		--statistics --extend-exclude="./build/" --extend-ignore=F722

down-dev:
	docker compose down

down-test:
	docker compose --file docker-compose.test.yml down

follow-fastapi:
	docker compose logs fastapi -f

fastapi-docker:
	./docker-build.sh polyneme/nmdc-runtime-fastapi nmdc_runtime/fastapi.Dockerfile

fastapi-deploy-spin:
	rancher kubectl rollout restart deployment/fastapi --namespace=nmdc-runtime-dev
	rancher kubectl rollout restart deployment/drs --namespace=nmdc-runtime-dev

dagster-docker:
	./docker-build.sh polyneme/nmdc-runtime-dagster nmdc_runtime/dagster.Dockerfile

dagster-deploy-spin:
	rancher kubectl rollout restart deployment/dagit --namespace=nmdc-runtime-dev
	rancher kubectl rollout restart deployment/dagit-readonly --namespace=nmdc-runtime-dev
	rancher kubectl rollout restart deployment/dagster-daemon --namespace=nmdc-runtime-dev

publish:
	invoke publish

docs-dev:
	mkdocs serve -a localhost:8080

nersc-ssh-tunnel:
	# bash ~/nersc-sshproxy.sh # https://docs.nersc.gov/connect/mfa/#sshproxy
	ssh -L27027:mongo-loadbalancer.nmdc-runtime-dev.development.svc.spin.nersc.org:27017 \
		dtn02.nersc.gov '/bin/bash -c "while [[ 1 ]]; do echo heartbeat; sleep 300; done"'

mongorestore-nmdcdb-lite-archive:
	wget https://portal.nersc.gov/project/m3408/meta/mongodumps/nmdcdb.lite.archive.gz
	mongorestore --host localhost:27018 --username admin --password root --drop --gzip \
		--archive=nmdcdb.lite.archive.gz
	rm nmdcdb.lite.archive.gz

.PHONY: init update-deps update up-dev down-dev follow-fastapi \
	fastapi-docker dagster-docker publish docs