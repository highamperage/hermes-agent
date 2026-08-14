"""Microsoft Foundry provider profile.

Azure Foundry exposes an OpenAI-compatible endpoint; users supply their own
base URL at setup since endpoints are per-resource.
"""

import json
import logging
import re
import urllib.parse
import urllib.request
from typing import Any, Optional

from providers import register_provider
from providers.base import ProviderProfile, _profile_user_agent

logger = logging.getLogger(__name__)


class AzureFoundryProviderProfile(ProviderProfile):
    """Azure Foundry provider profile supporting per-resource base URLs."""

    def fetch_models(
        self,
        *,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        timeout: float = 8.0,
        auth_mode: str = "api_key",
        **kwargs: Any,
    ) -> Optional[list[str]]:
        effective_base = (base_url or "").strip()
        if not effective_base:
            return None

        # Claude on Foundry is served under /anthropic, which has no /models
        # route (404). The resource-level OpenAI v1 catalog lists BOTH the
        # OpenAI-style and Anthropic-style deployments, so always discover
        # against /openai/v1 regardless of which wire the active model uses.
        if re.search(r"/anthropic(/v1(/messages)?)?/?$", effective_base, re.IGNORECASE):
            effective_base = re.sub(
                r"/anthropic(/v1(/messages)?)?/?$",
                "/openai/v1",
                effective_base,
                flags=re.IGNORECASE,
            )

        # Correctly form the /models URL for Azure v1 endpoints and legacy ?api-version endpoints, preserving query parameters
        parsed = urllib.parse.urlparse(effective_base)
        clean_path = re.sub(r"/deployments/[^/]+", "", parsed.path).rstrip("/")
        if not clean_path.endswith("/models"):
            clean_path = clean_path + "/models"

        url = urllib.parse.urlunparse(parsed._replace(path=clean_path))

        from hermes_cli.urllib_security import open_credentialed_url

        req = urllib.request.Request(url)
        if api_key:
            # Send exactly one auth header: api-key for API-key mode; Authorization: Bearer for Entra.
            if auth_mode in ("entra_id", "entra"):
                req.add_header("Authorization", f"Bearer {api_key}")
            else:
                req.add_header("api-key", api_key)

        req.add_header("Accept", "application/json")
        req.add_header("User-Agent", _profile_user_agent())
        for k, v in self.default_headers.items():
            req.add_header(k, v)

        try:
            with open_credentialed_url(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode())
            items = data if isinstance(data, list) else data.get("data", [])
            model_ids = [m["id"] for m in items if isinstance(m, dict) and "id" in m]
            return model_ids if model_ids else None
        except Exception as exc:
            logger.debug("fetch_models(azure-foundry): %s", exc)
            return None


azure_foundry = AzureFoundryProviderProfile(
    name="azure-foundry",
    aliases=("azure", "azure-ai-foundry", "azure-ai"),
    display_name="Azure Foundry",
    description="Microsoft Foundry - OpenAI-compatible endpoint (user-supplied base URL)",
    signup_url="https://ai.azure.com/",
    env_vars=("AZURE_FOUNDRY_API_KEY", "AZURE_FOUNDRY_BASE_URL"),
    base_url="",  # per-resource; user provides at setup
    auth_type="api_key",
)

register_provider(azure_foundry)
