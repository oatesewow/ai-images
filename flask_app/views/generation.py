from flask import Blueprint, request, render_template, redirect, url_for, flash
from ..services.approval_store import ensure_schema, insert_generation_rows

generation_bp = Blueprint("generation", __name__, template_folder="../templates")

@generation_bp.route("/")
def index():
    return render_template("generation.html")

@generation_bp.post("/ingest")
def ingest():
    # Expects a CSV uploaded from the generation step or JSON
    # Minimal: accept JSON array of rows matching insert_generation_rows schema
    payload = request.get_json(silent=True) or []
    ensure_schema()
    insert_generation_rows(payload)
    flash(f"Ingested {len(payload)} rows")
    return redirect(url_for("approval.pending"))
