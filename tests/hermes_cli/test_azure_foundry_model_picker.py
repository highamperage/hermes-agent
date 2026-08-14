"""Tests for live model discovery on the azure-foundry provider in /model picker.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from hermes_cli.auth import resolve_api_key_provider_credentials
from hermes_cli.models import provider_model_ids
from providers import get_provider_profile


def test_azure_foundry_fetch_models_url_formation_v1_and_legacy_api_version():
    """fetch_models correctly forms /models URL for v1 and legacy ?api-version endpoints, preserving query params."""
    profile = get_provider_profile("azure-foundry")
    assert profile is not None

    mock_response = MagicMock()
    mock_response.read.return_value = json.dumps(
        {
            "object": "list",
            "data": [
                {"id": "gpt-5.6-terra", "object": "model"},
                {"id": "gpt-5.6-luna", "object": "model"},
            ],
        }
    ).encode("utf-8")
    mock_response.__enter__.return_value = mock_response

    # Standard Azure v1 deployment path
    with patch("hermes_cli.urllib_security.open_credentialed_url", return_value=mock_response) as mock_open:
        models = profile.fetch_models(
            api_key="sk-test-key",
            base_url="https://test-resource.openai.azure.com/openai/deployments/gpt-5.6-terra/v1",
            auth_mode="api_key",
        )
        assert models == ["gpt-5.6-terra", "gpt-5.6-luna"]
        req = mock_open.call_args[0][0]
        assert req.full_url == "https://test-resource.openai.azure.com/openai/v1/models"
        # Rule 3: api-key header for API-key mode, NO Authorization header
        assert req.headers.get("Api-key") == "sk-test-key"
        assert "Authorization" not in req.headers

    # Legacy ?api-version endpoint preserving query parameters
    with patch("hermes_cli.urllib_security.open_credentialed_url", return_value=mock_response) as mock_open:
        models = profile.fetch_models(
            api_key="sk-test-key",
            base_url="https://test-resource.openai.azure.com/openai/deployments/gpt-5.6-terra?api-version=2024-02-15-preview&foo=bar",
            auth_mode="api_key",
        )
        assert models == ["gpt-5.6-terra", "gpt-5.6-luna"]
        req = mock_open.call_args[0][0]
        assert req.full_url == "https://test-resource.openai.azure.com/openai/models?api-version=2024-02-15-preview&foo=bar"


def test_azure_foundry_fetch_models_auth_header_entra_mode():
    """fetch_models sends Authorization: Bearer for Entra mode and NO api-key header."""
    profile = get_provider_profile("azure-foundry")
    assert profile is not None

    mock_response = MagicMock()
    mock_response.read.return_value = json.dumps(
        {"data": [{"id": "gpt-5.6-terra"}, {"id": "gpt-5.6-luna"}]}
    ).encode("utf-8")
    mock_response.__enter__.return_value = mock_response

    with patch("hermes_cli.urllib_security.open_credentialed_url", return_value=mock_response) as mock_open:
        models = profile.fetch_models(
            api_key="entra-jwt-token-123",
            base_url="https://test-resource.openai.azure.com/openai/v1",
            auth_mode="entra_id",
        )
        assert models == ["gpt-5.6-terra", "gpt-5.6-luna"]
        req = mock_open.call_args[0][0]
        # Rule 3: Authorization: Bearer for Entra mode, NO api-key header
        assert req.headers.get("Authorization") == "Bearer entra-jwt-token-123"
        assert "Api-key" not in req.headers


def test_azure_foundry_non_azure_model_config_guard(monkeypatch):
    """Rule 1: Config fallback for Azure is ONLY used when model.provider is exactly azure-foundry."""
    monkeypatch.delenv("AZURE_FOUNDRY_API_KEY", raising=False)
    monkeypatch.delenv("AZURE_FOUNDRY_BASE_URL", raising=False)

    non_azure_config = {
        "model": {
            "provider": "openai",
            "api_key": "sk-openai-secret-key",
            "base_url": "https://api.openai.com/v1",
            "auth_mode": "entra_id",
            "entra": {"scope": "custom-scope"},
        }
    }

    with patch("hermes_cli.config.load_config", return_value=non_azure_config):
        creds = resolve_api_key_provider_credentials("azure-foundry")
        # Must NOT leak sk-openai-secret-key or OpenAI base_url
        assert creds.get("api_key") == ""
        assert creds.get("base_url") == ""
        assert creds.get("auth_mode") == "api_key"


def test_azure_foundry_entra_discovery_uses_build_token_provider(monkeypatch):
    """Rule 2: Entra discovery uses real build_token_provider API from azure_identity_adapter."""
    monkeypatch.delenv("AZURE_FOUNDRY_API_KEY", raising=False)
    monkeypatch.delenv("AZURE_FOUNDRY_BASE_URL", raising=False)

    entra_azure_config = {
        "model": {
            "provider": "azure-foundry",
            "base_url": "https://entra-resource.openai.azure.com/openai/v1",
            "auth_mode": "entra_id",
            "entra": {"scope": "https://ai.azure.com/.default"},
        }
    }

    mock_token_provider = MagicMock(return_value="mock-entra-bearer-jwt")

    with patch("hermes_cli.config.load_config", return_value=entra_azure_config), patch(
        "agent.azure_identity_adapter.build_token_provider",
        return_value=mock_token_provider,
    ) as mock_build_fn:
        creds = resolve_api_key_provider_credentials("azure-foundry")
        assert creds.get("base_url") == "https://entra-resource.openai.azure.com/openai/v1"
        assert creds.get("api_key") == "mock-entra-bearer-jwt"
        assert creds.get("auth_mode") == "entra_id"
        mock_build_fn.assert_called_once()


def test_azure_foundry_entra_mode_ignores_lingering_api_key(monkeypatch):
    """When Entra mode is configured for azure-foundry, lingering AZURE_FOUNDRY_API_KEY is ignored."""
    monkeypatch.setenv("AZURE_FOUNDRY_API_KEY", "sk-lingering-static-key")
    monkeypatch.setenv("AZURE_FOUNDRY_BASE_URL", "https://entra-resource.openai.azure.com/openai/v1")

    entra_azure_config = {
        "model": {
            "provider": "azure-foundry",
            "base_url": "https://entra-resource.openai.azure.com/openai/v1",
            "auth_mode": "entra_id",
            "entra": {"scope": "https://ai.azure.com/.default"},
        }
    }

    mock_token_provider = MagicMock(return_value="fresh-entra-token-jwt")

    with patch("hermes_cli.config.load_config", return_value=entra_azure_config), patch(
        "agent.azure_identity_adapter.build_token_provider",
        return_value=mock_token_provider,
    ) as mock_build_fn:
        creds = resolve_api_key_provider_credentials("azure-foundry")
        assert creds.get("auth_mode") == "entra_id"
        assert creds.get("api_key") == "fresh-entra-token-jwt"
        assert creds.get("api_key") != "sk-lingering-static-key"
        mock_build_fn.assert_called_once()


def test_azure_foundry_entra_mode_failure_clears_api_key(monkeypatch):
    """When Entra mode is configured for azure-foundry and token resolution raises, api_key is empty, never lingering static key."""
    monkeypatch.setenv("AZURE_FOUNDRY_API_KEY", "sk-lingering-static-key")
    monkeypatch.setenv("AZURE_FOUNDRY_BASE_URL", "https://entra-resource.openai.azure.com/openai/v1")

    entra_azure_config = {
        "model": {
            "provider": "azure-foundry",
            "base_url": "https://entra-resource.openai.azure.com/openai/v1",
            "auth_mode": "entra_id",
            "entra": {"scope": "https://ai.azure.com/.default"},
        }
    }

    with patch("hermes_cli.config.load_config", return_value=entra_azure_config), patch(
        "agent.azure_identity_adapter.build_token_provider",
        side_effect=RuntimeError("Entra token acquisition failed"),
    ):
        creds = resolve_api_key_provider_credentials("azure-foundry")
        assert creds.get("auth_mode") == "entra_id"
        assert creds.get("api_key") == ""
        assert creds.get("api_key") != "sk-lingering-static-key"


def test_azure_foundry_allowlist_filtering_filters_many_ids_and_handles_missing(monkeypatch):
    """Live fetch returns only this resource's deployed models, in allowlist order."""
    monkeypatch.setenv("AZURE_FOUNDRY_API_KEY", "sk-azure-key")
    monkeypatch.setenv("AZURE_FOUNDRY_BASE_URL", "https://test-resource.openai.azure.com/openai/v1")

    profile = get_provider_profile("azure-foundry")
    assert profile is not None

    # Case 1: the endpoint returns the full Foundry catalog (400+ SKUs incl.
    # image/embedding models and undeployed Claude/GPT SKUs) -> keep only the
    # three deployments that exist on this resource.
    with patch.object(
        profile,
        "fetch_models",
        return_value=[
            "gpt-4o",
            "gpt-5.6-luna",
            "text-embedding-3-small",
            "claude-sonnet-5",
            "claude-opus-5",
            "gpt-5.6-terra",
            "dall-e-3",
        ],
    ):
        result = provider_model_ids("azure-foundry", force_refresh=True)
        assert result == ["gpt-5.6-terra", "gpt-5.6-luna", "claude-sonnet-5"]

    # Case 2: only some allowlisted ids present -> return just those.
    with patch.object(
        profile,
        "fetch_models",
        return_value=["gpt-4o", "gpt-5.6-luna", "text-embedding-3-small"],
    ):
        result = provider_model_ids("azure-foundry", force_refresh=True)
        assert result == ["gpt-5.6-luna"]

    # Case 3: Claude must survive the filter — it is served on the /anthropic
    # route and the old GPT-only allowlist silently dropped it.
    with patch.object(
        profile,
        "fetch_models",
        return_value=["claude-sonnet-5", "claude-opus-4-5", "dall-e-3"],
    ):
        result = provider_model_ids("azure-foundry", force_refresh=True)
        assert result == ["claude-sonnet-5"]


def test_azure_foundry_provider_model_ids_filters_allowlisted_deployments(monkeypatch):
    """provider_model_ids('azure-foundry') returns only deployed models from a mixed live response."""
    monkeypatch.setenv("AZURE_FOUNDRY_API_KEY", "sk-azure-key")
    monkeypatch.setenv("AZURE_FOUNDRY_BASE_URL", "https://test-resource.openai.azure.com/openai/v1")

    profile = get_provider_profile("azure-foundry")
    assert profile is not None

    with patch.object(
        profile,
        "fetch_models",
        return_value=["gpt-4o", "claude-sonnet-5", "text-embedding-3-small", "gpt-5.6-luna", "dall-e-3"],
    ) as mock_fetch:
        result = provider_model_ids("azure-foundry", force_refresh=True)
        assert result == ["gpt-5.6-luna", "claude-sonnet-5"]
        mock_fetch.assert_called_once_with(
            api_key="sk-azure-key",
            base_url="https://test-resource.openai.azure.com/openai/v1",
            auth_mode="api_key",
        )


def test_azure_foundry_model_picker_includes_all_deployments_and_keeps_terra(monkeypatch):
    """model_switch / list_authenticated_providers surfaces all discovered deployments for azure-foundry and keeps gpt-5.6-terra default."""
    monkeypatch.setenv("AZURE_FOUNDRY_API_KEY", "sk-azure-key")
    monkeypatch.setenv("AZURE_FOUNDRY_BASE_URL", "https://test-resource.openai.azure.com/openai/v1")

    with patch(
        "hermes_cli.models.provider_model_ids",
        return_value=["gpt-5.6-terra", "gpt-5.6-luna"],
    ):
        from hermes_cli.model_switch import list_authenticated_providers

        options = list_authenticated_providers(
            current_provider="azure-foundry",
            current_model="gpt-5.6-terra",
        )
        azure_row = next((r for r in options if r.get("slug") == "azure-foundry"), None)
        assert azure_row is not None
        assert azure_row["slug"] == "azure-foundry"
        assert "gpt-5.6-terra" in azure_row["models"]
        assert "gpt-5.6-luna" in azure_row["models"]


def test_non_azure_provider_fetch_models_without_auth_mode_kwarg(monkeypatch):
    """Generic API-key providers whose fetch_models signature does not accept auth_mode still receive live discovery without TypeError."""

    class StrictFakeNonAzureProfile:
        name = "strict-fake-provider"
        auth_type = "api_key"
        base_url = "https://api.fake.com/v1"
        fallback_models = ()

        def __init__(self):
            self.called_kwargs = None

        def fetch_models(self, *, api_key=None, base_url=None, timeout=8.0):
            # Strict signature — does NOT accept auth_mode or **kwargs
            self.called_kwargs = {"api_key": api_key, "base_url": base_url}
            return ["fake-model-alpha", "fake-model-beta"]

    fake_profile = StrictFakeNonAzureProfile()

    monkeypatch.setattr(
        "providers.get_provider_profile",
        lambda name: fake_profile if name == "strict-fake-provider" else None,
    )
    monkeypatch.setattr(
        "hermes_cli.auth.resolve_api_key_provider_credentials",
        lambda name: {"api_key": "fake-secret-key", "base_url": "https://api.fake.com/v1"},
    )

    result = provider_model_ids("strict-fake-provider", force_refresh=True)
    assert result == ["fake-model-alpha", "fake-model-beta"]
    assert fake_profile.called_kwargs == {
        "api_key": "fake-secret-key",
        "base_url": "https://api.fake.com/v1",
    }


def test_cached_provider_model_ids_filters_fresh_disk_cache_for_azure_foundry(monkeypatch, tmp_path):
    """cached_provider_model_ids('azure-foundry') filters pre-fix disk cache entries containing many model IDs."""
    import time
    from hermes_cli.models import cached_provider_model_ids, _credential_fingerprint

    cache_file = tmp_path / "provider_models_cache.json"
    monkeypatch.setattr("hermes_cli.models._provider_models_cache_path", lambda: cache_file)

    fp = _credential_fingerprint("azure-foundry")
    pre_fix_cache = {
        "azure-foundry": {
            "fp": fp,
            "at": time.time(),
            "models": [
                "gpt-4o",
                "claude-sonnet-5",
                "text-embedding-3-small",
                "gpt-5.6-luna",
                "dall-e-3",
                "gpt-3.5-turbo",
            ],
        }
    }
    cache_file.write_text(json.dumps(pre_fix_cache), encoding="utf-8")

    result = cached_provider_model_ids("azure-foundry", force_refresh=False)
    assert result == ["gpt-5.6-luna", "claude-sonnet-5"]
