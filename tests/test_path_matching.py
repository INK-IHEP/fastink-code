"""Unit tests for the shared path-matching helper used by the auth and
IP-whitelist middlewares.

Matching semantics (agreed design):
  - a pattern ending with "/"  -> PREFIX match  (e.g. "/api/v1/")
  - a pattern without trailing "/" -> EXACT match (e.g. "/api/v2/auth/get_token")

Both UserValidationMiddleware (skip_routers) and IPWhitelistMiddleware
(ip_controlled_routers) rely on this single helper so the two config lists
have identical, predictable semantics.
"""

from fastink.routers.headers import _path_matches


class TestPrefixPatterns:
    def test_trailing_slash_matches_prefix(self):
        assert _path_matches("/api/v1/foo/bar", ["/api/v1/"]) is True

    def test_trailing_slash_matches_itself_without_slash(self):
        # "/api/v1/" should still match the bare "/api/v1" request path
        assert _path_matches("/api/v1", ["/api/v1/"]) is True

    def test_trailing_slash_no_match_other_prefix(self):
        assert _path_matches("/api/v2/foo", ["/api/v1/"]) is False


class TestExactPatterns:
    def test_exact_matches_only_full_path(self):
        assert _path_matches("/api/v2/auth/get_token", ["/api/v2/auth/get_token"]) is True

    def test_exact_does_not_match_longer_path(self):
        # This is the key security tightening: an exact pattern must NOT
        # behave like a prefix, so a hypothetical sibling route is not caught.
        assert _path_matches("/api/v2/auth/get_token_extra", ["/api/v2/auth/get_token"]) is False

    def test_exact_does_not_match_subpath(self):
        assert _path_matches("/api/v2/auth/get_token/x", ["/api/v2/auth/get_token"]) is False

    def test_exact_no_match_different_path(self):
        assert _path_matches("/api/v2/auth/get_user", ["/api/v2/auth/get_token"]) is False


class TestMixedLists:
    def test_prefix_and_exact_coexist(self):
        patterns = [
            "/api/v1/",                       # prefix
            "/api/v2/auth/get_token",         # exact
            "/api/v2/auth/create_user",       # exact
        ]
        assert _path_matches("/api/v1/anything", patterns) is True
        assert _path_matches("/api/v2/auth/get_token", patterns) is True
        assert _path_matches("/api/v2/auth/create_user", patterns) is True
        # exact entries must not leak into prefix behaviour
        assert _path_matches("/api/v2/auth/get_tokenX", patterns) is False
        assert _path_matches("/api/v2/auth/get_permission", patterns) is False

    def test_empty_patterns_never_match(self):
        assert _path_matches("/api/v2/auth/get_token", []) is False

    def test_auth_prefix_still_matches_all_auth_when_present(self):
        # Backward-compat: a "/api/v2/auth/" prefix entry catches every
        # auth endpoint (this is the OLD skip_routers behaviour we are
        # moving away from, but the helper must still support it).
        assert _path_matches("/api/v2/auth/get_user", ["/api/v2/auth/"]) is True
