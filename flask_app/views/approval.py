from flask import Blueprint, render_template, request, redirect, url_for, flash
from ..services.approval_store import ensure_schema, list_pending, update_review

approval_bp = Blueprint("approval", __name__, template_folder="../templates")

@approval_bp.before_app_request
def _ensure_schema():
    ensure_schema()

@approval_bp.route("/pending")
def pending():
    page = int(request.args.get("page", 1))
    limit = 50
    offset = (page - 1) * limit
    rows = list_pending(limit=limit, offset=offset)
    return render_template("pending.html", rows=rows, page=page)

@approval_bp.post("/decision")
def decision():
    item_id = int(request.form["id"])
    action = request.form["action"]
    notes = request.form.get("notes", "")
    reviewer = request.form.get("reviewer", "")
    status = "approved" if action == "approve" else "rejected"
    update_review(item_id, status, reviewer, notes)
    flash(f"Updated item {item_id} -> {status}")
    return redirect(url_for("approval.pending"))
