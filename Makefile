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

down-dev:
	docker compose down

down-test:
	docker compose --file docker-compose.test.yml down

follow-fastapi:
	docker compose logs fastapi -f

fastapi-docker:
	./docker-build.sh polyneme/nmdc-runtime-fastapi nmdc_runtime/fastapi.Dockerfile

fastapi-deploy-spin:
	export RANCHER_TOKEN=`jq -r .Servers.rancherDefault.tokenKey ~/.rancher/cli2.json`
	export RANCHER_USERID=`curl -H "Authorization: Bearer $RANCHER_TOKEN" https://rancher2.spin.nersc.gov/v3/users?me=true | jq -r '.data[0].id'`
	rancher kubectl rollout restart deployment/fastapi --namespace=nmdc-runtime-dev
	rancher kubectl rollout restart deployment/drs --namespace=nmdc-runtime-dev

dagster-docker:
	./docker-build.sh polyneme/nmdc-runtime-dagster nmdc_runtime/dagster.Dockerfile

dagster-deploy-spin:
	export RANCHER_TOKEN=`jq -r .Servers.rancherDefault.tokenKey ~/.rancher/cli2.json`
	export RANCHER_USERID=`curl -H "Authorization: Bearer $RANCHER_TOKEN" https://rancher2.spin.nersc.gov/v3/users?me=true | jq -r '.data[0].id'`
	rancher kubectl rollout restart deployment/dagit --namespace=nmdc-runtime-dev
	rancher kubectl rollout restart deployment/dagster-daemon --namespace=nmdc-runtime-dev

publish:
	invoke publish

docs-dev:
	mkdocs serve -a localhost:8080

nersc-ssh-tunnel:
	# bash ~/nersc-sshproxy.sh # https://docs.nersc.gov/connect/mfa/#sshproxy
	ssh -L27027:mongo-loadbalancer.nmdc-runtime-dev.development.svc.spin.nersc.org:27017 \
		dtn01.nersc.gov '/bin/bash -c "while [[ 1 ]]; do echo heartbeat; sleep 300; done"'

.PHONY: init update-deps update up-dev down-dev follow-fastapi \
	fastapi-docker dagster-docker publish docs