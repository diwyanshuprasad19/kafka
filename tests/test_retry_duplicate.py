"""Retry / duplicate / transient classification unit tests."""

from checkpoint_platform.domain.exceptions import (
    DuplicateEventError,
    PermanentValidationError,
    TransientProcessingError,
    is_transient_error,
    retry_delay_seconds,
)


def test_duplicate_is_not_transient():
    assert is_transient_error(DuplicateEventError("x")) is False


def test_permanent_not_transient():
    assert is_transient_error(PermanentValidationError("bad")) is False


def test_explicit_transient():
    assert is_transient_error(TransientProcessingError("db timeout")) is True


def test_timeout_message_is_transient():
    assert is_transient_error(Exception("connection timed out")) is True
    assert is_transient_error(Exception("OperationalError: server closed")) is True


def test_backoff_grows():
    assert retry_delay_seconds(0, 500) == 0.5
    assert retry_delay_seconds(1, 500) == 1.0
    assert retry_delay_seconds(2, 500) == 2.0
    assert retry_delay_seconds(10, 500) == 30.0  # capped


def test_normalize_app_env():
    from checkpoint_platform.config.settings import normalize_app_env

    assert normalize_app_env("gcp") == "prod"
    assert normalize_app_env("local") == "local"
    assert normalize_app_env("docker") == "local"
