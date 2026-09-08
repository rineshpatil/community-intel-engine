import os

import pytest


@pytest.fixture(autouse=True)
def isolate_from_developer_config(tmp_path, monkeypatch):
    """Run every test in an empty directory with no CIE_* variables set.

    Settings loads .env from the current working directory. Without this, a
    real .env in the repo root leaks the developer's deployed configuration
    into the suite — which is exactly how two webhook tests started failing
    the moment a live stack was configured: the handler read the SSM
    parameter name from .env and tried to fetch a real secret instead of
    using the one the test had set.

    Tests must depend only on what they set themselves.
    """
    monkeypatch.chdir(tmp_path)
    for key in [k for k in os.environ if k.startswith("CIE_")]:
        monkeypatch.delenv(key, raising=False)
