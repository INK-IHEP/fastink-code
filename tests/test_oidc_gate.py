import fastink.auth.oidc as oidc_pkg


def test_should_mount_oidc_gates_on_mode(monkeypatch):
    cases = {"legacy": False, "dual": True, "oidc": True}
    for mode, expected in cases.items():
        monkeypatch.setattr(oidc_pkg, "get_auth_mode", lambda mode=mode: mode)
        assert oidc_pkg.should_mount_oidc() is expected, mode
