init:
	pip install -r requirements/main.txt
	pip install -r requirements/dev.txt
	pip install --editable .[dev]

update-deps:
    # --allow-unsafe pins packages considered unsafe: distribute, pip, setuptools.
	pip install --upgrade pip-tools pip setuptools
	pip-compile --upgrade --build-isolation --generate-hashes \
		--allow-unsafe --resolver=backtracking --strip-extras \
		--output-file requirements/main.txt \
		requirements/main.in
	pip-compile --allow-unsafe --upgrade --build-isolation --generate-hashes \
		--allow-unsafe --resolver=backtracking --strip-extras \
		--output-file requirements/dev.txt \
		requirements/dev.in

update: update-deps init

update-schema:
	pip-compile --upgrade-package nmdc-schema --build-isolation --generate-hashes \
		--allow-unsafe --resolver=backtracking --strip-extras \
		--output-file requirements/main.txt \
		requirements/main.in
	pip install -r requirements/main.txt

update-schema-yaml:
	# XXX depends on git checkout of nmdc-schema repo in sibling directory.
	# XXX you likely want to `git checkout vX.Y.Z`, where nmdc-schema==X.Y.Z is installed
	rm -rf nmdc_schema_yaml_src
	cp -r ../nmdc-schema/src/schema/ nmdc_schema_yaml_src

up-dev:
	docker-compose up --build --force-recreate --detach --remove-orphans

up-test:
	docker-compose --file docker-compose.test.yml \
		up --build --force-recreate --detach --remove-orphans

test-build:
	docker compose --file docker-compose.test.yml \
		up test --build --force-recreate --detach --remove-orphans

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
	docker-compose down

down-test:
	docker-compose --file docker-compose.test.yml down

follow-fastapi:
	docker-compose logs fastapi -f

fastapi-docker:
	./docker-build.sh microbiomedata/nmdc-runtime-fastapi nmdc_runtime/fastapi.Dockerfile

fastapi-deploy-spin:
	rancher kubectl rollout restart deployment/runtime-fastapi --namespace=nmdc-dev

dagster-docker:
	./docker-build.sh microbiomedata/nmdc-runtime-dagster nmdc_runtime/dagster.Dockerfile

dagster-deploy-spin:
	rancher kubectl rollout restart deployment/dagit --namespace=nmdc-dev
	rancher kubectl rollout restart deployment/dagster-daemon --namespace=nmdc-dev

publish:
	invoke publish

docs-dev:
	mkdocs serve -a localhost:8080

nersc-sshproxy:
	bash ~/nersc-sshproxy.sh # https://docs.nersc.gov/connect/mfa/#sshproxy

dev-nersc-ssh-tunnel:
	ssh -L28082:mongo-loadbalancer.nmdc-dev.development.svc.spin.nersc.org:27017 \
		dtn02.nersc.gov '/bin/bash -c "while [[ 1 ]]; do echo heartbeat; sleep 300; done"'

prod-nersc-ssh-tunnel:
	ssh -L27072:mongo-loadbalancer.nmdc.production.svc.spin.nersc.org:27017 \
		dtn02.nersc.gov '/bin/bash -c "while [[ 1 ]]; do echo heartbeat; sleep 300; done"'

mongorestore-nmdcdb-lite-archive:
	wget https://portal.nersc.gov/project/m3408/meta/mongodumps/nmdcdb.lite.archive.gz
	mongorestore --host localhost:27018 --username admin --password root --drop --gzip \
		--archive=nmdcdb.lite.archive.gz
	rm nmdcdb.lite.archive.gz

quick-blade:
	python -c "from nmdc_runtime.api.core.idgen import generate_id; print(f'nmdc:nt-11-{generate_id(length=8, split_every=0)}')"

.PHONY: init update-deps update up-dev down-dev follow-fastapi \
	fastapi-docker dagster-docker publish docs
