import manage


def test_force_updates_password(fake_db):
    email = "force-admin@example.com"
    # Create initial admin
    rc = manage.create_admin_account(email, "InitialPass1!", role="admin", force=False)
    assert rc is True

    # Read current password_hash
    cur = fake_db.conn.cursor()
    cur.execute("SELECT password_hash FROM users WHERE email = ?", (email,))
    row = cur.fetchone()
    assert row is not None
    old_hash = row[0]

    # Force update password
    rc2 = manage.create_admin_account(email, "NewPass2!", role="admin", force=True)
    assert rc2 is True

    # Verify password hash changed
    cur.execute("SELECT password_hash FROM users WHERE email = ?", (email,))
    row2 = cur.fetchone()
    assert row2 is not None
    new_hash = row2[0]
    assert new_hash != old_hash
