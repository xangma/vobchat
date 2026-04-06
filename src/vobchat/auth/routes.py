from __future__ import annotations

from uuid import uuid4

from flask import (
    Blueprint,
    abort,
    flash,
    redirect,
    render_template_string,
    request,
    session,
    url_for,
)
from flask_login import current_user, login_required, login_user, logout_user

from vobchat.auth.extensions import db
from vobchat.auth.models import User
from vobchat.auth.templates import LOGIN_PAGE_NO_SIGNUP
from vobchat.core.settings import get_settings


bp = Blueprint("auth", __name__)


def _route_prefix() -> str:
    return get_settings().dash.route_prefix


def _url_base() -> str:
    return get_settings().dash.url_base_pathname


@bp.route("/login", methods=["GET"])
def login_page():
    if current_user.is_authenticated:
        return redirect(f"{_route_prefix() or ''}/")
    return render_template_string(LOGIN_PAGE_NO_SIGNUP, base=_url_base())


@bp.route("/signup", methods=["GET", "POST"])
def signup():
    abort(404)


@bp.route("/login", methods=["POST"])
def login():
    email = request.form.get("email", "").strip().lower()
    password = request.form.get("password", "")
    next_url = request.form.get("next") or request.args.get("next") or ""

    user = db.session.scalar(db.select(User).filter_by(email=email))
    if user and user.verify_password(password):
        login_user(user)
        session["login_session_id"] = str(uuid4())
        if next_url.startswith("/") and not next_url.startswith("//"):
            return redirect(next_url)
        return redirect(f"{_route_prefix() or ''}/")

    flash("Invalid credentials")
    return redirect(url_for(".login_page"))


@bp.route("/logout")
@login_required
def logout():
    logout_user()
    session.pop("login_session_id", None)
    return redirect(url_for("auth.login_page"))


def install_auth_guards(server, route_prefix: str | None = None) -> None:
    if getattr(server, "_vobchat_auth_guard_installed", False):
        return

    route_prefix = route_prefix if route_prefix is not None else _route_prefix()
    safe_subpaths = ("/login",)

    @server.before_request
    def protect_dash():
        path = request.path

        if not path.startswith(route_prefix):
            return None

        if any(path.startswith(f"{route_prefix}{subpath}") for subpath in safe_subpaths):
            return None

        if route_prefix == "":
            if path == "/login":
                return None
            if path == "/" and not current_user.is_authenticated:
                return redirect(url_for("auth.login_page", next=path))

        if not current_user.is_authenticated:
            return redirect(url_for("auth.login_page", next=path))

        if "login_session_id" not in session:
            session["login_session_id"] = str(uuid4())
        return None

    server._vobchat_auth_guard_installed = True
