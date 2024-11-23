# Use Poetry to install Python dependencies.
init:
	poetry install

# TODO: Document this target.
up-dev:
	docker compose up --build --force-recreate --detach --remove-orphans

# TODO: Document this target.
dev-reset-db:
	docker compose \
		exec mongo /bin/bash -c "./app_tests/mongorestore-nmdc-testdb.sh"

# TODO: Document this target.
up-test:
	docker compose --file docker-compose.test.yml \
		up --build --force-recreate --detach --remove-orphans

# TODO: Document this target.
test-build:
	docker compose --file docker-compose.test.yml build test

# TODO: Document this target.
test-dbinit:
	docker compose --file docker-compose.test.yml \
		exec mongo /bin/bash -c "/mongorestore-nmdc-testdb.sh"

# TODO: Document this target.
test-run:
	docker compose --file docker-compose.test.yml run test

# TODO: Document this target.
test: test-build test-run

# TODO: Document this target.
black:
	black nmdc_runtime

# TODO: Document this target.
lint:
	# Python syntax errors or undefined names
	flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics --extend-ignore=F722
	# exit-zero treats all errors as warnings. The GitHub editor is 127 chars wide
	flake8 . --count --exit-zero --max-complexity=10 --max-line-length=127 \
		--statistics --extend-exclude="./build/" --extend-ignore=F722

# Define variables containing Python package version numbers read from the Poetry lock file.
POETRY_LOCKED_FLAKE8_VERSION := $(shell grep -A 1 'name = "flake8"' poetry.lock | sed -ne 's/^version = "\(.*\)"/\1/p')
POETRY_LOCKED_BLACK_VERSION  := $(shell grep -A 1 'name = "black"'  poetry.lock | sed -ne 's/^version = "\(.*\)"/\1/p')

# Install lint and black via pip, without installing other dependencies.
init-lint-and-black:
	pip install flake8==$(POETRY_LOCKED_FLAKE8_VERSION)
	pip install black==$(POETRY_LOCKED_BLACK_VERSION)

# TODO: Document this target.
down-dev:
	docker compose down

# TODO: Document this target.
down-test:
	docker compose --file docker-compose.test.yml down --volumes

# TODO: Document this target.
follow-fastapi:
	docker compose logs fastapi -f

# TODO: Document this target.
fastapi-deploy-spin:
	rancher kubectl rollout restart deployment/runtime-fastapi --namespace=nmdc-dev

# TODO: Document this target.
dagster-deploy-spin:
	rancher kubectl rollout restart deployment/dagit --namespace=nmdc-dev
	rancher kubectl rollout restart deployment/dagster-daemon --namespace=nmdc-dev

# TODO: Document this target.
publish:
	invoke publish

# TODO: Document this target.
docs-dev:
	mkdocs serve -a localhost:8080

# TODO: Document this target.
nersc-sshproxy:
	bash ./nersc-sshproxy.sh -u ${NERSC_USERNAME} # https://docs.nersc.gov/connect/mfa/#sshproxy

# TODO: Document this target.
nersc-mongo-tunnels:
	ssh -L27072:mongo-loadbalancer.nmdc.production.svc.spin.nersc.org:27017 \
		-L28082:mongo-loadbalancer.nmdc-dev.development.svc.spin.nersc.org:27017 \
		-L27092:mongo-loadbalancer.nmdc-dev.production.svc.spin.nersc.org:27017 \
		-o ServerAliveInterval=60 \
		${NERSC_USERNAME}@dtn02.nersc.gov

# TODO: Document this target.
mongorestore-nmdc-db:
	mkdir -p /tmp/remote-mongodump/nmdc
	# SSH into the remote server, stream the dump directory as a gzipped tar archive, and extract it locally.
	ssh -i ~/.ssh/nersc ${NERSC_USERNAME}@dtn01.nersc.gov \
		'tar -czf - -C /global/cfs/projectdirs/m3408/nmdc-mongodumps/dump_nmdc-prod_2024-07-29_20-12-07/nmdc .' \
		| tar -xzv -C /tmp/remote-mongodump/nmdc
	mongorestore -v -h localhost:27018 -u admin -p root --authenticationDatabase=admin \
		--drop --nsInclude='nmdc.*' --dir /tmp/remote-mongodump

# TODO: Document this target.
quick-blade:
	python -c "from nmdc_runtime.api.core.idgen import generate_id; print(f'nmdc:nt-11-{generate_id(length=8, split_every=0)}')"

.PHONY: init up-dev down-dev follow-fastapi \
	publish docs
