import inspect
import app


def test_api_recent_applicants_sets_branch_name_default():
    src = inspect.getsource(app.api_recent_applicants)
    assert (
        "'branch_name': r.get('branch_name') or 'All Branches'" in src
        or "'branch_name': r.get('branch_name') or 'All Branches'" in src.replace('"', "'")
    ), "api_recent_applicants should default branch_name to 'All Branches' when branch is None"
