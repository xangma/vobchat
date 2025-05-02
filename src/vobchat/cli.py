# src/vobchat/cli.py
import click
from flask.cli import with_appcontext
from sqlalchemy.exc import IntegrityError
from vobchat.models import db, User

@click.command("create-user")
@click.argument("email")
@click.password_option()
@with_appcontext
def create_user(email, password):
    user = User.create(email.strip().lower(), password)
    db.session.add(user)
    try:
        db.session.commit()
        click.echo("✔ User created")
    except IntegrityError:
        db.session.rollback()
        click.echo("✖ That e-mail already exists", err=True)

def register_commands(app):
    app.cli.add_command(create_user)