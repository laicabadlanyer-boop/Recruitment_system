import pytest

from tests.test_manage_integration import FakeDB


@pytest.fixture
def fake_db(monkeypatch):
    db = FakeDB()
    import utils.database as dbmod
    import manage as mg

    monkeypatch.setattr(dbmod, "get_db", lambda: db)
    # Also patch the get_db used directly in manage module
    monkeypatch.setattr(mg, "get_db", lambda: db)
    # Avoid schema compatibility checks that expect Flask app context
    monkeypatch.setattr(mg, "ensure_schema_compatibility", lambda: True)
    return db
