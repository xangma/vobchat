# src/vobchat/cli.py
import click
from getpass import getpass
from flask.cli import with_appcontext
from sqlalchemy.exc import IntegrityError
from geoalchemy2 import Geometry
from vobchat.models import db, User, pwd_ctx

@click.command("add-user")
@click.argument("email")
@click.password_option(prompt=True, confirmation_prompt=True)
@with_appcontext
def add_user(email, password):
    """
    Create **or** reset-password for an e-mail account.
    Prompts twice to avoid typos.
    """
    email = email.strip().lower()

    user = db.session.scalar(db.select(User).filter_by(email=email))
    if user:                         # existing account → reset password
        user.password_hash = pwd_ctx.hash(password)
        verb = "Password reset"
    else:                            # brand-new user
        user = User.create(email, password)
        db.session.add(user)
        verb = "User created"

    try:
        db.session.commit()
        click.secho(f"✔ {verb}", fg="green")
    except IntegrityError:
        db.session.rollback()
        click.secho("✖ Database error", fg="red", err=True)

def register_commands(app):
    app.cli.add_command(add_user)
