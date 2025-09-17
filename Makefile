# Spin up the development stack.
up-dev:
	docker compose up --build --force-recreate --detach --remove-orphans

# Spin down the development stack.
down-dev:
	docker compose down

# Restores the MongoDB dump residing in `./tests/nmdcdb` on the Docker host, into the MongoDB server in the dev stack.
reset-db-dev:
	docker compose \
		exec mongo /bin/bash -c "/mongorestore-nmdc-testdb.sh"

# Uses Docker Compose to build and spin up the stack upon which the `test` container (i.e. the test runner) depends.
up-test:
	docker compose --file docker-compose.test.yml \
		up --build --force-recreate --detach --remove-orphans

# Tears down the `test` stack, including removing data volumes such as that containing the test MongoDB database.
down-test:
	docker compose --file docker-compose.test.yml down --volumes

# Restores the MongoDB dump residing in `./tests/nmdcdb` on the Docker host, into the MongoDB server in the test stack.
reset-db-test:
	docker compose --file docker-compose.test.yml \
		exec mongo /bin/bash -c "/mongorestore-nmdc-testdb.sh"

# Run tests on the started `test` stack, passing `ARGS` to `pytest` (see Tip below).
#
# Tip: If you append `ARGS=` and a file path to the `make` command, pytest will run only the tests defined in that file.
#
#      Some examples, using `make test`:
#      (You can also use `make run-test` if are sure you don't need to reset global state)
#
#      To run only the tests defined in `tests/test_api/test_endpoints.py`:
#      ```
#      $ make test ARGS="tests/test_api/test_endpoints.py"
#      ```
#
#      To run only the test `test_find_data_objects_for_study_having_one` in `tests/test_api/test_endpoints.py`:
#      ```
#      $ make test ARGS="-k 'test_find_data_objects_for_study_having_one'"
#      ```
#
run-test:
	docker compose --file docker-compose.test.yml exec -it test \
		./.docker/wait-for-it.sh fastapi:8000 --strict --timeout=300 -- \
			uv run --active \
				pytest --cov=nmdc_runtime \
				       --doctest-modules \
				       --ignore=util/load_testing \
				       $(ARGS)

# Uses Docker Compose to
# 1. Ensure the `test` stack is torn down, including data volumes such as that containing the test MongoDB database.
# 2. Build and spin up the stack upon which the `test` container (i.e. the test runner) depends.
# 3. Run tests on the `test` container, passing `ARGS` to `pytest` (see Tip in comment above for `run-test` target).
test: down-test up-test run-test

# Format Python code using `black`.
# TODO: Migrate from `black` to `ruff`.
black:
	uv run --active black nmdc_runtime

# Lint Python code using `flake8`.
# TODO: Migrate from `flake8` to `ruff`.
lint:
	# Python syntax errors or undefined names
	uv run --active flake8 --count --select=E9,F63,F7,F82 --show-source --statistics --extend-ignore=F722 \
		./nmdc_runtime ./tests
	# exit-zero treats all errors as warnings. The GitHub editor is 127 chars wide
	uv run --active flake8 --count --exit-zero --max-complexity=10 --max-line-length=127 \
		--statistics --extend-exclude="./build/" --extend-ignore=F722 \
		./nmdc_runtime ./tests

# Build the MkDocs documentation website and serve it at http://localhost:8080.
# Docs: https://www.mkdocs.org/user-guide/cli/#mkdocs-serve
docs-dev:
	mkdocs serve --config-file docs/mkdocs.yml --dev-addr localhost:8080

# 🙋 Prerequisites:
#
# 1. The `PATH_TO_NERSC_SSHPROXY` environment variable is set to the path of
#    an executable copy of the `sshproxy` program maintained by NERSC.
#    You can read about and download that program at the following URL:
#    https://docs.nersc.gov/connect/mfa/#sshproxy
#    
# 2. The `NERSC_USERNAME` environment variable is set to your NERSC username.
#
nersc-sshproxy:
	${PATH_TO_NERSC_SSHPROXY} -u ${NERSC_USERNAME}

nersc-mongo-tunnels:
	ssh -L27072:mongo-loadbalancer.nmdc.production.svc.spin.nersc.org:27017 \
		-L27092:mongo-loadbalancer.nmdc-dev.production.svc.spin.nersc.org:27017 \
		-o ServerAliveInterval=60 \
		${NERSC_USERNAME}@dtn02.nersc.gov

DEV_STACK_HOST_MACHINE_PORT_MONGO:=$(shell cat .env | grep DEV_STACK_HOST_MACHINE_PORT_MONGO= | cut -d= -f2)

mongorestore-nmdc-db:
	mkdir -p /tmp/remote-mongodump/nmdc
	# Optionally, manually update MONGO_REMOTE_DUMP_DIR env var:
	# ```bash
	# export MONGO_REMOTE_DUMP_DIR=$(ssh -i ~/.ssh/nersc -q ${NERSC_USERNAME}@dtn01.nersc.gov 'bash -s ' < util/get_latest_nmdc_prod_dump_dir.sh 2>/dev/null)
	# ```
	# Rsync the remote dump directory items of interest:
	rsync -av --no-perms \
		--exclude='*_agg\.*' \
		--exclude='operations\.*' \
		--exclude='_*' \
		--exclude='ids_nmdc_sys0\.*' \
		--exclude='query_runs\.*' \
		--exclude='fs\.*' \
		--exclude="alldocs\.*" \
		-e "ssh -i ~/.ssh/nersc" \
		${NERSC_USERNAME}@dtn01.nersc.gov:${MONGO_REMOTE_DUMP_DIR}/nmdc/ \
		/tmp/remote-mongodump/nmdc
	# Restore from `rsync`ed local directory:
	mongorestore -v -h localhost:$(DEV_STACK_HOST_MACHINE_PORT_MONGO) -u admin -p root --authenticationDatabase=admin \
		--drop --nsInclude='nmdc.*' --gzip --dir /tmp/remote-mongodump

quick-blade:
	uv run --active python -c "from nmdc_runtime.api.core.idgen import generate_id; print(f'nmdc:nt-11-{generate_id(length=8, split_every=0)}')"

# List of Make targets that do not represent files being created.
# Note: I think _most_ of the targets in this Makefile meet that criterion,
#       despite them not being listed here.
.PHONY: up-dev down-dev publish docs quick-blade

# Note: The leading hyphen in each command below tells `make` to ignore whether
#       the command fails (i.e. returns a non-zero exit code).
docker-clean:
	# Down the dev stack with volumes
	-docker compose down --volumes --remove-orphans
	# Down the test stack with volumes
	-docker compose --file docker-compose.test.yml down --volumes --remove-orphans
	# Remove any dangling images from this project
	-docker image prune -f
	# Remove the project-specific networks
	-docker network rm nmdc-runtime-dev nmdc-runtime-test 2>/dev/null || true
	# Remove the project-specific volumes
	-docker volume rm nmdc-runtime-dev_mongo_data nmdc-runtime-dev_postgres_data nmdc-runtime-test_mongo_data nmdc-runtime-test_postgres_data 2>/dev/null || true
