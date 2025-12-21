import inspect
import app


def test_fetch_jobs_for_user_includes_global_jobs():
    src = inspect.getsource(app.fetch_jobs_for_user)
    assert (
        "(j.branch_id = %s OR j.branch_id IS NULL)" in src
    ), "fetch_jobs_for_user should include global jobs for branch-scoped HR"


def test_coalesce_branch_name_updated():
    # search for the display fallback in the app source
    src = open("c:\\xampp\\htdocs\\Recruitment System\\app.py", "r", encoding="utf-8").read()
    assert "COALESCE(b.branch_name, 'All Branches')" in src, "Branch display fallback should be 'All Branches'"
