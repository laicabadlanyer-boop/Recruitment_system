import manage


def test_rotate_all_writes_otp_files(fake_db, tmp_path):
    # Create two admins
    emails = ["a1@example.com", "a2@example.com"]
    for e in emails:
        manage.create_admin_account(e, "Initial1!", role="admin")

    otp_dir = tmp_path / "otps"
    rc = manage.main(["rotate_all_admins", "--generate-password", "--otp-dir", str(otp_dir)])
    assert rc == 0

    # Check that files exist
    for e in emails:
        safe = e.replace("@", "_at_").replace(".", "_")
        p = otp_dir / f"{safe}.txt"
        assert p.exists()
        content = p.read_text().strip()
        assert len(content) >= 8


def test_rotate_all_sends_email(fake_db, monkeypatch):
    emails = ["mail1@example.com"]
    for e in emails:
        manage.create_admin_account(e, "Initial1!", role="admin")

    sent = []

    def fake_send_email(recipient, subject, body, html_body=None):
        sent.append((recipient, subject, body))

    monkeypatch.setattr(manage, "send_email", fake_send_email)
    rc = manage.main(["rotate_all_admins", "--generate-password", "--email-otp"])
    assert rc == 0
    assert len(sent) == 1
    assert sent[0][0] == emails[0]
