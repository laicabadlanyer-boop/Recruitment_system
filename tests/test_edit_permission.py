import app
from flask import render_template


def render_applications(app_module, applications):
    """Render the applications template within a request context and return HTML."""
    with app_module.app.test_request_context("/"):
        html = render_template(
            "applicant/applications.html",
            applications=applications,
            analytics={"total_applications": len(applications), "status_breakdown": {}},
            status_filter="",
        )
        return html


def test_edit_allowed_when_not_viewed():
    applications = [
        {
            "application_id": 42,
            "job_id": 10,
            "job_title": "Test Job",
            "branch_name": "HQ",
            "position_title": "Tester",
            "status": "pending",
            "applied_at": "2025-12-20",
            "submitted_at": "2025-12-20",
            "has_interview": False,
            "interview_date": None,
            "interview_mode": None,
            "interview_location": None,
            "is_viewed": False,
        }
    ]
    html = render_applications(app, applications)

    assert "Edit" in html
    assert "edit=42" in html


def test_edit_blocked_when_viewed():
    applications = [
        {
            "application_id": 43,
            "job_id": 11,
            "job_title": "Viewed Job",
            "branch_name": "HQ",
            "position_title": "Tester",
            "status": "pending",
            "applied_at": "2025-12-20",
            "submitted_at": "2025-12-20",
            "has_interview": False,
            "interview_date": None,
            "interview_mode": None,
            "interview_location": None,
            "is_viewed": True,
        }
    ]
    html = render_applications(app, applications)

    assert "Cannot edit - already viewed by HR" in html
    assert "disabled" in html
