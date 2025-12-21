import pytest
import manage


def test_parser_contains_create_admin():
    p = manage.create_parser()
    h = p.format_help()
    assert "create_admin" in h


def test_create_admin_requires_email():
    parser = manage.create_parser()
    with pytest.raises(SystemExit):
        # No args for subcommand should trigger error due to required --email
        parser.parse_args(["create_admin"])
