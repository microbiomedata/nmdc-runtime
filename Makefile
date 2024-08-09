init:
	pip install -r requirements/main.txt
	pip install -r requirements/dev.txt
	pip install --editable .[dev]

# Updates Python dependencies based upon the contents of the `requirements/main.in` and `requirements/dev.in` files.
#
# To omit the `--upgrade` option (included by default) when running `pip-compile`:
# ```
# $ make update-deps UPDATE_DEPS_MAIN_UPGRADE_OPT='' UPDATE_DEPS_DEV_UPGRADE_OPT=''
# ```
UPDATE_DEPS_MAIN_UPGRADE_OPT ?= --upgrade
UPDATE_DEPS_DEV_UPGRADE_OPT ?= --upgrade
update-deps:
    # --allow-unsafe pins packages considered unsafe: distribute, pip, setuptools.
	pip install --upgrade pip-tools pip setuptools
	pip-compile $(UPDATE_DEPS_MAIN_UPGRADE_OPT) --build-isolation \
		--allow-unsafe --resolver=backtracking --strip-extras \
		--output-file requirements/main.txt \
		requirements/main.in
	pip-compile --allow-unsafe $(UPDATE_DEPS_DEV_UPGRADE_OPT) --build-isolation \
		--allow-unsafe --resolver=backtracking --strip-extras \
		--output-file requirements/dev.txt \
		requirements/dev.in

update: update-deps init

up-dev:
	docker compose up --build --force-recreate --detach --remove-orphans

dev-reset-db:
	docker compose \
		exec mongo /bin/bash -c "./app_tests/mongorestore-nmdc-testdb.sh"

up-test:
	docker compose --file docker-compose.test.yml \
		up --build --force-recreate --detach --remove-orphans

test-build:
	docker compose --file docker-compose.test.yml build test

test-dbinit:
	docker compose --file docker-compose.test.yml \
		exec mongo /bin/bash -c "/mongorestore-nmdc-testdb.sh"

test-run:
	docker compose --file docker-compose.test.yml run test

test: test-build test-run

black:
	black nmdc_runtime

lint:
	# Python syntax errors or undefined names
	flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics --extend-ignore=F722
	# exit-zero treats all errors as warnings. The GitHub editor is 127 chars wide
	flake8 . --count --exit-zero --max-complexity=10 --max-line-length=127 \
		--statistics --extend-exclude="./build/" --extend-ignore=F722

PIP_PINNED_FLAKE8 := $(shell grep 'flake8==' requirements/dev.txt)
PIP_PINNED_BLACK := $(shell grep 'black==' requirements/dev.txt)

init-lint-and-black:
	pip install $(PIP_PINNED_FLAKE8)
	pip install $(PIP_PINNED_BLACK)

down-dev:
	docker compose down

down-test:
	docker compose --file docker-compose.test.yml down --volumes

follow-fastapi:
	docker compose logs fastapi -f

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
	bash ./nersc-sshproxy.sh -u ${NERSC_USERNAME} # https://docs.nersc.gov/connect/mfa/#sshproxy

nersc-mongo-tunnels:
	ssh -L27072:mongo-loadbalancer.nmdc.production.svc.spin.nersc.org:27017 \
		-L28082:mongo-loadbalancer.nmdc-dev.development.svc.spin.nersc.org:27017 \
		-L27092:mongo-loadbalancer.nmdc-dev.production.svc.spin.nersc.org:27017 \
		-o ServerAliveInterval=60 \
		${NERSC_USERNAME}@dtn02.nersc.gov

mongorestore-nmdc-db:
	mkdir -p /tmp/remote-mongodump/nmdc
	# SSH into the remote server, stream the dump directory as a gzipped tar archive, and extract it locally.
	ssh -i ~/.ssh/nersc ${NERSC_USERNAME}@dtn01.nersc.gov \
		'tar -czf - -C /global/cfs/projectdirs/m3408/nmdc-mongodumps/dump_nmdc-prod_2024-07-29_20-12-07/nmdc .' \
		| tar -xzv -C /tmp/remote-mongodump/nmdc
	mongorestore -v -h localhost:27018 -u admin -p root --authenticationDatabase=admin \
		--drop --nsInclude='nmdc.*' --dir /tmp/remote-mongodump

quick-blade:
	python -c "from nmdc_runtime.api.core.idgen import generate_id; print(f'nmdc:nt-11-{generate_id(length=8, split_every=0)}')"

.PHONY: init update-deps update up-dev down-dev follow-fastapi \
	publish docs
