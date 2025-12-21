import manage


def test_list_admins_prints_none_when_empty(fake_db, capsys):
    # Ensure no admins
    rc = manage.main(["list_admins"])
    captured = capsys.readouterr()
    assert "No admin accounts found" in captured.out


def test_list_admins_shows_admin(fake_db, capsys):
    # Create an admin first
    email = "show-admin@example.com"
    manage.create_admin_account(email, "ShowPass1!", role="admin")
    rc = manage.main(["list_admins"])
    captured = capsys.readouterr()
    assert email in captured.out
