#!/bin/bash

set -euo pipefail

exec gunicorn --worker-tmp-dir /dev/shm --workers=2 \
              --threads=4 --worker-class gthread \
              --log-file=- --bind 0.0.0.0:8000 nmdc_runtime.api.main:app