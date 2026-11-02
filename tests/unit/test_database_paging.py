"""Page-size bounds are enforced in the data layer, not only at the API edge."""

import asyncio

import pytest

from app.database import MAX_PAGE_SIZE, TransactionFilters, list_rule_set_versions, list_transactions


def no_session():
    raise AssertionError("an invalid page request must be rejected before touching the database")


@pytest.mark.parametrize("limit", [0, -1, -100, MAX_PAGE_SIZE + 1, 10**9, True, 2.5, "10"])
def test_list_transactions_rejects_out_of_range_limits(limit):
    with pytest.raises(ValueError, match="limit must be"):
        asyncio.run(list_transactions(no_session, TransactionFilters(), limit, None))


@pytest.mark.parametrize("limit", [0, -1, MAX_PAGE_SIZE + 1])
def test_list_rule_set_versions_rejects_out_of_range_limits(limit):
    with pytest.raises(ValueError, match="limit must be"):
        asyncio.run(list_rule_set_versions(no_session, "fraud-default", limit, None))


@pytest.mark.parametrize("before_version", [0, -3])
def test_list_rule_set_versions_rejects_non_positive_before_version(before_version):
    with pytest.raises(ValueError, match="before_version"):
        asyncio.run(list_rule_set_versions(no_session, "fraud-default", 10, before_version))
