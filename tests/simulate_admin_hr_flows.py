from app import app


def run_simulation():
    with app.test_client() as client:
        # Simulate admin viewing an applicant
        with client.session_transaction() as sess:
            sess["logged_in"] = True
            sess["auth_user_id"] = 1
            sess["user_id"] = 1
            sess["user_role"] = "admin"
            sess["user_email"] = "admin@example.com"
            sess["user_name"] = "Admin"

        print("\n--- Admin: GET /admin/applicants/1 ---")
        resp = client.get("/admin/applicants/1")
        print("Status:", resp.status_code)
        print("Body snippet:", resp.get_data(as_text=True)[:400])

        # Simulate HR trying to archive application id 1
        with client.session_transaction() as sess:
            sess["logged_in"] = True
            sess["auth_user_id"] = 2
            sess["user_id"] = 2
            sess["user_role"] = "hr"
            sess["user_email"] = "hr@example.com"
            sess["user_name"] = "HR User"
            # provide branch scope if necessary in session
            # sess['branch_id'] = 1

        print("\n--- HR: POST /hr/applicants/1/archive ---")
        resp = client.post("/hr/applicants/1/archive", data={})
        print("Status:", resp.status_code)
        print("Body snippet:", resp.get_data(as_text=True)[:400])


if __name__ == "__main__":
    run_simulation()
