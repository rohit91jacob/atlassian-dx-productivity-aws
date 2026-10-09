"""Stable pseudonyms for people.

The curated layer never stores Jira usernames, display names or GitHub logins: each identity is
replaced by ``u_`` + the first 12 hex characters of HMAC-SHA256(salt, "<source>:<login>").
Reviewer-load metrics only need "the same person" across rows, not who that person is.

This is *pseudonymisation*, not anonymisation. The source data is public, so anyone who knows
the salt can re-identify a login by hashing candidates. Set ``DX_PSEUDONYM_SALT`` to a private
value for deployments where that matters; the default salt is public and only suitable for demos.
"""

from __future__ import annotations

import hashlib
import hmac


def pseudonymise(source: str, login: str | None, salt: str) -> str | None:
    if not login:
        return None
    digest = hmac.new(salt.encode(), f"{source}:{login.lower()}".encode(), hashlib.sha256)
    return f"u_{digest.hexdigest()[:12]}"
