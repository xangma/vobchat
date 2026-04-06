from flask_login import LoginManager
from flask_sqlalchemy import SQLAlchemy
from passlib.context import CryptContext


db = SQLAlchemy()
login_manager = LoginManager()
login_manager.login_view = "auth.login_page"
pwd_ctx = CryptContext(schemes=["argon2"], deprecated="auto")
