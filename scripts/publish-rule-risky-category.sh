#!/usr/bin/env bash

set -euo pipefail

TXN_ID="rule-risky-category-$(date + %s)"

./scripts/publish-transaction.sh \
    --transaction-id "$TXN_ID" \
    --user-id "rule-user-risky" \
    --amount 6000 \
    ==category GAMBLING

echo "Published risky-category scenerio transaction: $TXN_ID"
echo "Expected trigger: RISKY_CATEGORY_LIMIT"