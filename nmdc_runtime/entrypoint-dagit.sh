#!/bin/bash

set -euo pipefail

exec dagit -h 0.0.0.0 -p 3000 -w workspace.yaml
