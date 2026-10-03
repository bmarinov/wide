#!/usr/bin/env bash
set -euo pipefail

coverage_profile="coverage.out"
html_report="coverage.html"

# storetest is a contract suite for stores outside this module; nothing here runs it.
coverpkg=$(go list ./... | grep -v /storetest | paste -sd, -)

go test -coverpkg="$coverpkg" -coverprofile="$coverage_profile" ./...
go tool cover -func="$coverage_profile"

if [[ "${1:-}" == "--html" ]]; then
    go tool cover -html="$coverage_profile" -o "$html_report"
    echo "HTML report saved as $html_report"
fi
