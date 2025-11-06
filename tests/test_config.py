"""Tests for nmdc_runtime.config module."""

import importlib
import os
import pytest
from nmdc_runtime import config


class TestSentryConfig:
    """Tests for Sentry configuration variables."""

    def test_sentry_dsn_default_empty_string(self, monkeypatch):
        """Test that SENTRY_DSN defaults to empty string when not set."""
        monkeypatch.delenv("SENTRY_DSN", raising=False)
        # Reload the config module to apply the change
        importlib.reload(config)
        assert config.SENTRY_DSN == ""

    def test_sentry_dsn_from_env(self, monkeypatch):
        """Test that SENTRY_DSN reads from environment variable."""
        test_dsn = "https://example@sentry.io/123456"
        monkeypatch.setenv("SENTRY_DSN", test_dsn)
        importlib.reload(config)
        assert config.SENTRY_DSN == test_dsn

    def test_sentry_environment_default_unknown(self, monkeypatch):
        """Test that SENTRY_ENVIRONMENT defaults to 'unknown' when not set."""
        monkeypatch.delenv("SENTRY_ENVIRONMENT", raising=False)
        importlib.reload(config)
        assert config.SENTRY_ENVIRONMENT == "unknown"

    def test_sentry_environment_from_env(self, monkeypatch):
        """Test that SENTRY_ENVIRONMENT reads from environment variable."""
        test_env = "production"
        monkeypatch.setenv("SENTRY_ENVIRONMENT", test_env)
        importlib.reload(config)
        assert config.SENTRY_ENVIRONMENT == test_env

    def test_is_sentry_enabled_default_false(self, monkeypatch):
        """Test that IS_SENTRY_ENABLED defaults to False when not set."""
        monkeypatch.delenv("IS_SENTRY_ENABLED", raising=False)
        importlib.reload(config)
        assert config.IS_SENTRY_ENABLED is False

    def test_is_sentry_enabled_true(self, monkeypatch):
        """Test that IS_SENTRY_ENABLED is True when set to 'true'."""
        monkeypatch.setenv("IS_SENTRY_ENABLED", "true")
        importlib.reload(config)
        assert config.IS_SENTRY_ENABLED is True

    def test_is_sentry_enabled_false(self, monkeypatch):
        """Test that IS_SENTRY_ENABLED is False when set to 'false'."""
        monkeypatch.setenv("IS_SENTRY_ENABLED", "false")
        importlib.reload(config)
        assert config.IS_SENTRY_ENABLED is False

    def test_is_sentry_enabled_case_insensitive(self, monkeypatch):
        """Test that IS_SENTRY_ENABLED is case-insensitive."""
        monkeypatch.setenv("IS_SENTRY_ENABLED", "TRUE")
        importlib.reload(config)
        assert config.IS_SENTRY_ENABLED is True

    def test_sentry_traces_sample_rate_default(self, monkeypatch):
        """Test that SENTRY_TRACES_SAMPLE_RATE defaults to 0.1 when not set."""
        monkeypatch.delenv("SENTRY_TRACES_SAMPLE_RATE", raising=False)
        importlib.reload(config)
        assert config.SENTRY_TRACES_SAMPLE_RATE == 0.1

    def test_sentry_traces_sample_rate_from_env(self, monkeypatch):
        """Test that SENTRY_TRACES_SAMPLE_RATE reads from environment variable."""
        monkeypatch.setenv("SENTRY_TRACES_SAMPLE_RATE", "0.5")
        importlib.reload(config)
        assert config.SENTRY_TRACES_SAMPLE_RATE == 0.5

    def test_sentry_profiles_sample_rate_default(self, monkeypatch):
        """Test that SENTRY_PROFILES_SAMPLE_RATE defaults to 0.1 when not set."""
        monkeypatch.delenv("SENTRY_PROFILES_SAMPLE_RATE", raising=False)
        importlib.reload(config)
        assert config.SENTRY_PROFILES_SAMPLE_RATE == 0.1

    def test_sentry_profiles_sample_rate_from_env(self, monkeypatch):
        """Test that SENTRY_PROFILES_SAMPLE_RATE reads from environment variable."""
        monkeypatch.setenv("SENTRY_PROFILES_SAMPLE_RATE", "0.75")
        importlib.reload(config)
        assert config.SENTRY_PROFILES_SAMPLE_RATE == 0.75


class TestIsEnvVarTrue:
    """Tests for the is_env_var_true utility function."""

    def test_is_env_var_true_undefined(self, monkeypatch):
        """Test behavior when environment variable is undefined."""
        monkeypatch.delenv("TEST_VAR", raising=False)
        assert config.is_env_var_true("TEST_VAR") is False

    def test_is_env_var_true_with_default_true(self, monkeypatch):
        """Test that default value is used when environment variable is undefined."""
        monkeypatch.delenv("TEST_VAR", raising=False)
        assert config.is_env_var_true("TEST_VAR", default="true") is True

    def test_is_env_var_true_with_true(self, monkeypatch):
        """Test that 'true' value returns True."""
        monkeypatch.setenv("TEST_VAR", "true")
        assert config.is_env_var_true("TEST_VAR") is True

    def test_is_env_var_true_with_false(self, monkeypatch):
        """Test that 'false' value returns False."""
        monkeypatch.setenv("TEST_VAR", "false")
        assert config.is_env_var_true("TEST_VAR") is False

    def test_is_env_var_true_case_insensitive(self, monkeypatch):
        """Test that the check is case-insensitive."""
        monkeypatch.setenv("TEST_VAR", "TRUE")
        assert config.is_env_var_true("TEST_VAR") is True

    def test_is_env_var_true_non_boolean(self, monkeypatch):
        """Test that non-boolean strings return False."""
        monkeypatch.setenv("TEST_VAR", "potato")
        assert config.is_env_var_true("TEST_VAR") is False
