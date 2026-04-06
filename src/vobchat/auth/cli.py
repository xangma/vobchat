import click
from flask.cli import with_appcontext
from sqlalchemy.exc import IntegrityError

from vobchat.auth.extensions import db, pwd_ctx
from vobchat.auth.models import User, ensure_auth_schema


@click.command("add-user")
@click.argument("email")
@click.password_option(prompt=True, confirmation_prompt=True)
@with_appcontext
def add_user(email, password):
    ensure_auth_schema()

    email = email.strip().lower()
    user = db.session.scalar(db.select(User).filter_by(email=email))
    if user:
        user.password_hash = pwd_ctx.hash(password)
        verb = "Password reset"
    else:
        user = User.create(email, password)
        db.session.add(user)
        verb = "User created"

    try:
        db.session.commit()
        click.secho(f"✔ {verb}", fg="green")
    except IntegrityError:
        db.session.rollback()
        click.secho("✖ Database error", fg="red", err=True)


@click.command("init-db")
@with_appcontext
def init_db():
    try:
        ensure_auth_schema()
        click.secho("✔ Database initialised", fg="green")
    except Exception as exc:
        click.secho(f"✖ Failed to initialise DB: {exc}", fg="red", err=True)


def register_auth_commands(app):
    app.cli.add_command(add_user)
    app.cli.add_command(init_db)
