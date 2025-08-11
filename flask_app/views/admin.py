from flask import Blueprint, request, render_template, redirect, url_for, flash
from ..services.approval_store import ensure_schema
from ..services.generation_service import run_batch

admin_bp = Blueprint("admin", __name__, template_folder="../templates")


@admin_bp.route("/admin/generate", methods=["GET", "POST"])
def generate():
    if request.method == "POST":
        category = request.form.get("category", "Beach Holidays")
        limit = int(request.form.get("limit", 5))
        try:
            ensure_schema()
            count = run_batch(category, limit)
            flash(f"Queued {count} images to pending for category '{category}'")
            return redirect(url_for("approval.pending"))
        except Exception as e:
            flash(f"Generation failed: {e}")
            return render_template("admin_generate.html"), 500
    return render_template("admin_generate.html")

