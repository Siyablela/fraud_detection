import os

import pytest

from app.settings import Settings, clear_settings_cache, get_settings


def test_missing_required_variables_fail_fast_and_are_all_named():
    for name in ("DATABASE_URL", "REDIS_URL", "JWT_AUDIENCE"):
        os.environ.pop(name)
    with pytest.raises(RuntimeError) as error:
        Settings.from_env()
    assert "DATABASE_URL" in str(error.value)
    assert "REDIS_URL" in str(error.value)
    assert "JWT_AUDIENCE" in str(error.value)


def test_secret_can_be_read_from_file(tmp_path):
    secret_file = tmp_path / "database-url"
    secret_file.write_text("postgresql://vault:s3cret@db/fraud\n", encoding="utf-8")
    os.environ.pop("DATABASE_URL")
    os.environ["DATABASE_URL_FILE"] = str(secret_file)
    assert Settings.from_env().database_url == "postgresql://vault:s3cret@db/fraud"


def test_unreadable_secret_file_is_an_error(tmp_path):
    os.environ.pop("DATABASE_URL")
    os.environ["DATABASE_URL_FILE"] = str(tmp_path / "missing")
    with pytest.raises(RuntimeError, match="DATABASE_URL_FILE"):
        Settings.from_env()


@pytest.mark.parametrize("algorithms", ["HS256", "none", "RS256,HS512"])
def test_symmetric_or_none_jwt_algorithms_are_refused(algorithms):
    os.environ["JWT_ALGORITHMS"] = algorithms
    with pytest.raises(RuntimeError, match="JWT_ALGORITHMS"):
        Settings.from_env()


def test_invalid_value_error_names_the_field_but_not_the_value():
    os.environ["DB_POOL_SIZE"] = "not-a-number-s3cret"
    with pytest.raises(RuntimeError) as error:
        Settings.from_env()
    assert "DB_POOL_SIZE" in str(error.value)
    assert "s3cret" not in str(error.value)


def test_defaults_and_derived_values():
    settings = Settings.from_env()
    assert settings.kafka_dlq_topic_name == "transactions.dlq"
    assert settings.cors_allowed_origins == []
    assert settings.scope_rules_write == "fraud:rules:write"
    assert settings.scope_rules_write != settings.scope_transactions_read


def test_csv_lists_are_parsed():
    os.environ["CORS_ALLOWED_ORIGINS"] = "https://a.example, https://b.example"
    assert Settings.from_env().cors_allowed_origins == ["https://a.example", "https://b.example"]


def test_backoff_bounds_must_be_ordered():
    os.environ["WORKER_RETRY_INITIAL_BACKOFF_SECONDS"] = "10"
    os.environ["WORKER_RETRY_MAX_BACKOFF_SECONDS"] = "1"
    with pytest.raises(RuntimeError, match="BACKOFF"):
        Settings.from_env()


def test_get_settings_is_cached_until_cleared():
    first = get_settings()
    os.environ["RULE_SET_KEY"] = "other"
    assert get_settings() is first
    clear_settings_cache()
    assert get_settings().rule_set_key == "other"
