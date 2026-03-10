# src/vobchat/cli.py
import click
from flask.cli import with_appcontext
from sqlalchemy.exc import IntegrityError
from vobchat.models import db, User, pwd_ctx, Base


@click.command("add-user")
@click.argument("email")
@click.password_option(prompt=True, confirmation_prompt=True)
@with_appcontext
def add_user(email, password):
    """
    Create **or** reset-password for an e-mail account.
    Prompts twice to avoid typos.
    """
    # Ensure tables exist (idempotent)
    try:
        db.create_all()
        # Ensure Declarative Base tables are created as well (User inherits from Base)
        try:
            engine = db.engine  # requires app context
            Base.metadata.create_all(bind=engine)
        except Exception:
            pass
    except Exception:
        pass

    email = email.strip().lower()

    user = db.session.scalar(db.select(User).filter_by(email=email))
    if user:  # existing account → reset password
        user.password_hash = pwd_ctx.hash(password)
        verb = "Password reset"
    else:  # brand-new user
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
    """Initialise the auth database (SQLite/Postgres) by creating tables."""
    try:
        db.create_all()
        try:
            engine = db.engine
            Base.metadata.create_all(bind=engine)
        except Exception:
            pass
        click.secho("✔ Database initialised", fg="green")
    except Exception as e:
        click.secho(f"✖ Failed to initialise DB: {e}", fg="red", err=True)


def register_commands(app):
    app.cli.add_command(add_user)
    app.cli.add_command(init_db)
