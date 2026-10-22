#!/usr/bin/env bash

set -euo pipefail

TXN_ID="rule-high-value-$(date + %s)"

./scripts/publish-transaction.sh \
    --transaction-id "$TXN_ID" \
    --user-id "rule-user-high" \
    --amount 15000 \
    ==category RETAIL

echo "Published high-value scenerio transaction: $TXN_ID"
echo "Expected trigger: HIGH_VALUE_THRESHOLD"