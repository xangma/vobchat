from flask_login import login_required

from vobchat.auth.cli import add_user, init_db, register_auth_commands
from vobchat.auth.extensions import db, login_manager, pwd_ctx
from vobchat.auth.models import Base, User, ensure_auth_schema
from vobchat.auth.routes import bp as auth_blueprint, install_auth_guards

__all__ = [
    "Base",
    "User",
    "add_user",
    "auth_blueprint",
    "db",
    "ensure_auth_schema",
    "init_db",
    "install_auth_guards",
    "login_manager",
    "login_required",
    "pwd_ctx",
    "register_auth_commands",
]
