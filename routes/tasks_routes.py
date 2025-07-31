# routes/tasks_routes.py

from flask import Blueprint, jsonify
from build_mapping import build_mapping, engine

tasks_bp = Blueprint("tasks", __name__)

@tasks_bp.route("/tasks/build-mapping")
def task_build_mapping():
    rows = build_mapping(engine, enable_fuzzy=False)
    return jsonify({"status": "ok", "rows": rows})
