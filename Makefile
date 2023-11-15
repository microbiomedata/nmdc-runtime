init:
	pip install -r requirements/main.txt
	pip install -r requirements/dev.txt
	pip install --editable .[dev]

update-deps:
    # --allow-unsafe pins packages considered unsafe: distribute, pip, setuptools.
	pip install --upgrade pip-tools pip setuptools
	pip-compile --upgrade --build-isolation \
		--allow-unsafe --resolver=backtracking --strip-extras \
		--output-file requirements/main.txt \
		requirements/main.in
	pip-compile --allow-unsafe --upgrade --build-isolation \
		--allow-unsafe --resolver=backtracking --strip-extras \
		--output-file requirements/dev.txt \
		requirements/dev.in

update: update-deps init

up-dev:
	docker-compose up --build --force-recreate --detach --remove-orphans

dev-reset-db:
	docker compose \
		exec mongo /bin/bash -c "./app_tests/mongorestore-nmdc-testdb.sh"

up-test:
	docker-compose --file docker-compose.test.yml \
		up --build --force-recreate --detach --remove-orphans

test-build:
	docker compose --file docker-compose.test.yml \
		up test --build --force-recreate --detach --remove-orphans

test-dbinit:
	docker compose --file docker-compose.test.yml \
		exec mongo /bin/bash -c "/mongorestore-nmdc-testdb.sh"

test-run:
	docker compose --file docker-compose.test.yml run test

test: test-build test-run

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

fastapi-deploy-spin:
	rancher kubectl rollout restart deployment/runtime-fastapi --namespace=nmdc-dev

dagster-deploy-spin:
	rancher kubectl rollout restart deployment/dagit --namespace=nmdc-dev
	rancher kubectl rollout restart deployment/dagster-daemon --namespace=nmdc-dev

publish:
	invoke publish

docs-dev:
	mkdocs serve -a localhost:8080

nersc-sshproxy:
	bash ~/nersc-sshproxy.sh # https://docs.nersc.gov/connect/mfa/#sshproxy

nersc-mongo-tunnels:
	ssh -L27072:mongo-loadbalancer.nmdc.production.svc.spin.nersc.org:27017 \
		-L28082:mongo-loadbalancer.nmdc-dev.development.svc.spin.nersc.org:27017 \
		-L27092:mongo-loadbalancer.nmdc-dev.production.svc.spin.nersc.org:27017 \
		-o ServerAliveInterval=60 \
		dtn02.nersc.gov

mongorestore-nmdcdb-lite-archive:
	wget https://portal.nersc.gov/project/m3408/meta/mongodumps/nmdcdb.lite.archive.gz
	mongorestore --host localhost:27018 --username admin --password root --drop --gzip \
		--archive=nmdcdb.lite.archive.gz
	rm nmdcdb.lite.archive.gz

quick-blade:
	python -c "from nmdc_runtime.api.core.idgen import generate_id; print(f'nmdc:nt-11-{generate_id(length=8, split_every=0)}')"

.PHONY: init update-deps update up-dev down-dev follow-fastapi \
	publish docs
