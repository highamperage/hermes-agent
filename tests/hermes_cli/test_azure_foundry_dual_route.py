"""Azure Foundry serves GPT and Claude on different routes of one resource.

``/openai/v1`` speaks the Responses/Chat wire, ``/anthropic`` speaks the native
Messages wire.  A single configured ``model.base_url`` must therefore be
re-pointed per model family, or whichever family the stored suffix does not
match fails (``400 Unknown model`` for GPT on the Anthropic route, 404 for
Claude on the OpenAI route).
"""

import pytest

from hermes_cli.models import azure_foundry_model_base_url, is_anthropic_model_name
from hermes_cli.runtime_provider import _resolve_azure_foundry_runtime

ROOT = "https://ai-foundry-scus-01.services.ai.azure.com"


@pytest.mark.parametrize("model", ["claude-sonnet-5", "Claude-Opus-5", "anthropic/claude-opus-5"])
def test_anthropic_names_detected(model):
    assert is_anthropic_model_name(model)


@pytest.mark.parametrize("model", ["gpt-5.6-terra", "gpt-4o", "o3-mini", "", None])
def test_non_anthropic_names_rejected(model):
    assert not is_anthropic_model_name(model)


@pytest.mark.parametrize("configured", [f"{ROOT}/anthropic", f"{ROOT}/openai/v1", ROOT])
@pytest.mark.parametrize(
    "model,expected_suffix",
    [("claude-sonnet-5", "/anthropic"), ("gpt-5.6-terra", "/openai/v1")],
)
def test_route_derives_from_model_not_stored_suffix(configured, model, expected_suffix):
    assert azure_foundry_model_base_url(configured, model) == ROOT + expected_suffix


def test_non_azure_base_url_untouched():
    assert azure_foundry_model_base_url("https://api.openai.com/v1", "gpt-4o") == "https://api.openai.com/v1"


@pytest.mark.parametrize(
    "model,expected_mode,expected_suffix",
    [
        ("claude-sonnet-5", "anthropic_messages", "/anthropic"),
        ("gpt-5.6-terra", "codex_responses", "/openai/v1"),
    ],
)
def test_runtime_routes_both_families_from_one_config(monkeypatch, model, expected_mode, expected_suffix):
    """A config left over from a Claude session must not poison a GPT run."""
    monkeypatch.setenv("AZURE_FOUNDRY_API_KEY", "test-key")
    runtime = _resolve_azure_foundry_runtime(
        requested_provider="azure-foundry",
        model_cfg={
            "provider": "azure-foundry",
            "base_url": f"{ROOT}/anthropic",
            "api_mode": "anthropic_messages",
            "default": "claude-sonnet-5",
        },
        explicit_api_key="test-key",
        target_model=model,
    )
    assert runtime["api_mode"] == expected_mode
    assert runtime["base_url"] == ROOT + expected_suffix


def test_model_discovery_rewrites_anthropic_base_to_openai_catalog():
    """`/anthropic` has no `/models` route (404).

    Discovery must fall back to the resource's OpenAI v1 catalog, which lists
    BOTH wire families, or the picker shows zero models whenever the active
    model is Claude.
    """
    from providers import get_provider_profile

    profile = get_provider_profile("azure-foundry")
    assert profile is not None

    captured = {}

    class _Resp:
        def __enter__(self_inner):
            return self_inner

        def __exit__(self_inner, *a):
            return False

        def read(self_inner):
            return b'{"data": [{"id": "claude-sonnet-5"}, {"id": "gpt-5.6-luna"}]}'

    def _fake_open(req, timeout=None):
        captured["url"] = req.full_url
        return _Resp()

    import hermes_cli.urllib_security as sec

    orig = sec.open_credentialed_url
    sec.open_credentialed_url = _fake_open
    try:
        out = profile.fetch_models(api_key="k", base_url=f"{ROOT}/anthropic")
    finally:
        sec.open_credentialed_url = orig

    assert captured["url"] == f"{ROOT}/openai/v1/models"
    assert out == ["claude-sonnet-5", "gpt-5.6-luna"]
