#!/usr/bin/env bash

set -euo pipefail

BASE_TS=$(date + %s)
USER_ID="rule-user-velocity-amount"

# Default rule config trigger velocity amount when recent_amount_sum + current > 5000.

for i in 1 2 3; do
    TXN_ID=""rule-velocity-amount-${BASE_TS}-${i}
    ./scripts/publish-transaction.sh \
        --transaction-id "$TXN_ID" \
        --user-id "$USER_ID" \
        --amount 2000 \
        --category RETAIL \
        --timestamp "$((BASE_TS + i))"
done

echo "Published velocity-amount scenerio for user: $USER_ID"
echo "Expected trigger on later events: VELOCITY_AMOUNT"