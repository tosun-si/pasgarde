#!/usr/bin/env bash

set -e
set -o pipefail
set -u

echo "####Running pylint quality code check."

pipenv run lint asgarde --ignore-paths="asgarde/tests*" || pipenv run lintexit $?

if [ $? -eq 1 ]; then
  echo "An error occurred while running pylint." >&2
  exit 1
fi