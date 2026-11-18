from pathlib import Path

from flask import Flask, g, jsonify
from werkzeug.exceptions import BadRequest, HTTPException, MethodNotAllowed, NotFound, RequestEntityTooLarge

from app.config import DEFAULTS, resolve_database
from app.db import init_db
from app.errors import AppError
from app.ratelimit import RateLimiter
from app.routes import register_routes


def create_app(overrides=None):
    app = Flask(__name__)
    app.config.update(DEFAULTS)
    if overrides:
        app.config.update(overrides)

    project_root = Path(__file__).resolve().parent.parent
    app.config["DATABASE"] = resolve_database(app.config["DATABASE"], project_root)
    init_db(app.config["DATABASE"])

    window = app.config["RATE_WINDOW_SECONDS"]
    app.extensions["limiters"] = {
        "single": RateLimiter(app.config["SINGLE_RATE_LIMIT"], window),
        "bulk": RateLimiter(app.config["BULK_RATE_LIMIT"], window),
    }
    register_routes(app)
    _register_handlers(app)
    return app


def _register_handlers(app):
    @app.errorhandler(AppError)
    def handle_app_error(error):
        body = {"error": error.code, "message": error.message}
        if error.details:
            body["details"] = error.details
        return jsonify(body), error.status

    @app.errorhandler(RequestEntityTooLarge)
    def handle_too_large(_error):
        return jsonify({"error": "payload_too_large", "message": "Request body exceeds 8 MB"}), 413

    @app.errorhandler(BadRequest)
    def handle_bad_request(_error):
        return jsonify({"error": "bad_request", "message": "Request body must be valid JSON"}), 400

    @app.errorhandler(NotFound)
    def handle_not_found(_error):
        return jsonify({"error": "not_found", "message": "Route not found"}), 404

    @app.errorhandler(MethodNotAllowed)
    def handle_method(_error):
        return jsonify({"error": "method_not_allowed", "message": "Method not allowed"}), 405

    @app.errorhandler(HTTPException)
    def handle_http(error):
        return jsonify({"error": "http_error", "message": error.description}), error.code

    @app.after_request
    def add_rate_headers(response):
        limit = getattr(g, "rate_limit", None)
        if limit is not None:
            response.headers["X-RateLimit-Limit"] = str(limit)
            response.headers["X-RateLimit-Remaining"] = str(getattr(g, "rate_remaining", 0))
        return response
