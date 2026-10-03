"""Validate the narrow GitHub OIDC credential format used by manual recording."""

import os
from urllib.parse import parse_qs, urlsplit


def validate_github_credentials(data, identity, project):
    provider = identity.wif_provider
    expected = "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/" + identity.email + ":generateAccessToken"
    allowed_keys = {"type", "audience", "subject_token_type", "token_url", "credential_source",
                    "service_account_impersonation_url", "quota_project_id", "universe_domain"}
    if (not provider or set(data) - allowed_keys or data.get("type") != "external_account"
            or data.get("audience") != "//iam.googleapis.com/" + provider
            or data.get("subject_token_type") != "urn:ietf:params:oauth:token-type:jwt"
            or data.get("token_url") != "https://sts.googleapis.com/v1/token"
            or data.get("service_account_impersonation_url") != expected
            or data.get("quota_project_id", project) != project
            or data.get("universe_domain", "googleapis.com") != "googleapis.com"):
        raise ValueError("WIF configuration does not match the explicitly selected provider and identity")
    source = data.get("credential_source", {})
    if not isinstance(source, dict) or not isinstance(source.get("headers"), dict):
        raise ValueError("Invalid GitHub credential source")
    url = urlsplit(source.get("url", ""))
    runner_url = urlsplit(os.environ.get("ACTIONS_ID_TOKEN_REQUEST_URL", ""))
    query = parse_qs(url.query, keep_blank_values=True)
    runner_query = parse_qs(runner_url.query, keep_blank_values=True)
    runner_query["audience"] = [provider]
    if (os.environ.get("GITHUB_ACTIONS") != "true"
            or os.environ.get("GITHUB_EVENT_NAME") != "workflow_dispatch"
            or os.environ.get("GITHUB_REF") != "refs/heads/main"
            or url.scheme != "https" or url.username or url.password or url.port
            or not (url.hostname or "").endswith(".actions.githubusercontent.com")
            or url.fragment or (url.scheme, url.netloc, url.path) != (runner_url.scheme, runner_url.netloc, runner_url.path)
            or query != runner_query or set(source) != {"url", "headers", "format"}
            or source["format"] != {"type": "json", "subject_token_field_name": "value"}
            or set(source["headers"]) != {"Authorization"}
            or not isinstance(source["headers"]["Authorization"], str)
            or not source["headers"]["Authorization"].startswith("Bearer ")
            or not source["headers"]["Authorization"][7:]):
        raise ValueError("WIF source must be the current main-branch manual GitHub runner's OIDC endpoint")
    return dict(data, quota_project_id=project)
