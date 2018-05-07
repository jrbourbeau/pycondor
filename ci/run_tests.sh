#!/usr/bin/env bash

set -e

echo "Running tests..."
echo ""
pytest --cov pycondor

ls -la

echo "Running flake8..."
echo ""
flake8 pycondor
