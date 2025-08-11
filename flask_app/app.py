import os
import sys
from flask import Flask, redirect, url_for
from dotenv import load_dotenv

def create_app() -> Flask:
    # Load .env early
    load_dotenv()
    app = Flask(__name__)
    app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret")

    # Blueprints
    try:
        from .views.dashboard import dashboard_bp
        from .views.generation import generation_bp
        from .views.approval import approval_bp
        from .views.admin import admin_bp
    except ImportError:
        # Allow running as a script: python flask_app/app.py
        base_dir = os.path.dirname(os.path.dirname(__file__))
        if base_dir not in sys.path:
            sys.path.insert(0, base_dir)
        from flask_app.views.dashboard import dashboard_bp
        from flask_app.views.generation import generation_bp
        from flask_app.views.approval import approval_bp
        from flask_app.views.admin import admin_bp

    app.register_blueprint(dashboard_bp)
    app.register_blueprint(generation_bp, url_prefix="/generate")
    app.register_blueprint(approval_bp, url_prefix="/approval")
    app.register_blueprint(admin_bp)

    @app.route("/")
    def index():
        return redirect(url_for("approval.pending"))

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=True)


