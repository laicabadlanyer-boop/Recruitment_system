import manage


def test_parser_has_new_commands():
    p = manage.create_parser()
    h = p.format_help()
    assert "list_admins" in h
    assert "rotate_admin_password" in h
