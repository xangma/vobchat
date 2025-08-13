# src/vobchat/models.py
from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin
from passlib.context import CryptContext
from authlib.integrations.flask_client import OAuth
from flask import Blueprint, render_template_string, redirect, url_for, request, session, flash
from flask_login import LoginManager, login_user, logout_user, login_required, current_user
from vobchat.assets.loginpage import LOGIN_PAGE_NO_SIGNUP, SIGNUP_FORM_HTML
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String
import os

db = SQLAlchemy()

pwd_ctx = CryptContext(
    schemes=["argon2"],       # argon2id is OWASP’s 1st choice  [oai_citation:0‡OWASP Cheat Sheet Series](https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html?utm_source=chatgpt.com) [oai_citation:1‡passlib.readthedocs.io](https://passlib.readthedocs.io/en/stable/lib/passlib.hash.argon2.html?utm_source=chatgpt.com)
    deprecated="auto"
)

class Base(DeclarativeBase):
    pass

class User(Base, UserMixin):
    __tablename__ = "users"
    __allow_unmapped__ = True          # silence Pylance re: UserMixin attrs

    id:            Mapped[int]  = mapped_column(primary_key=True)
    email:         Mapped[str]  = mapped_column(String(255), nullable=False)
    password_hash: Mapped[str]  = mapped_column(String(256), nullable=False)

    @classmethod
    def create(cls, email: str, raw_password: str) -> "User":
        return cls(
            email=email,
            password_hash=pwd_ctx.hash(raw_password)
        )

    def verify_password(self, raw_password: str) -> bool:
        return pwd_ctx.verify(raw_password, self.password_hash)
def register_app_routes(server):
    """Register app routes for authentication and login."""
    


bp   = Blueprint("auth", __name__)
lm   = LoginManager()           # initialise in create_app()
lm.login_view = "auth.login_page"

@lm.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))

@bp.route("/login", methods=["GET"])
def login_page():
    """Initial GET page shown to unauthenticated users."""
    # Use Flask-Login state to decide; avoids loops with stale session ids
    if current_user.is_authenticated:
        DASH_PREFIX = os.getenv("DASH_URL_BASE", "/app").rstrip("/")
        return redirect(DASH_PREFIX + "/")          # already logged in
    return render_template_string(LOGIN_PAGE_NO_SIGNUP)

# ---------- sign-up -------------------------------------------------
@bp.route("/signup", methods=["GET", "POST"])
def signup():
    abort(404)
    # if request.method == "GET":
    #     return render_template_string(SIGNUP_FORM_HTML)

    # email = request.form["email"].strip().lower()
    # pwd   = request.form["password"]

    # # Try to create the user; unique index on lower(email) enforces 1-per-address
    # user = User.create(email, pwd)
    # db.session.add(user)
    # try:
    #     db.session.commit()          # succeeds if it’s a new e-mail
    # except IntegrityError:
    #     db.session.rollback()        # quietly ignore duplicate
    #     # (optional) sleep(0.05) to equalise timing, but usually not needed

    # # Same message for both code paths
    # flash("If an account with that e-mail exists, you can now log in.")
    # return redirect(url_for(".login_page"))

# ---------- login ---------------------------------------------------
@bp.route("/login", methods=["POST"])
def login():
    email = request.form.get("email", "").strip().lower()
    pwd   = request.form.get("password", "")
    next_url = request.form.get("next") or request.args.get("next") or ""

    user = db.session.scalar(db.select(User).filter_by(email=email))
    if user and user.verify_password(pwd):
        login_user(user)
        DASH_PREFIX = os.getenv("DASH_URL_BASE", "/app").rstrip("/")
        # Basic safety: only allow local relative redirects
        if next_url and next_url.startswith("/") and not next_url.startswith("//"):
            return redirect(next_url)
        return redirect(DASH_PREFIX + "/")

    flash("Invalid credentials")
    return redirect(url_for(".login_page"))

# ---------- logout --------------------------------------------------
@bp.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("auth.login_page"))
