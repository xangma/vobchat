from __future__ import annotations

from flask_login import UserMixin
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from vobchat.auth.extensions import db, login_manager, pwd_ctx


class Base(DeclarativeBase):
    pass


class User(Base, UserMixin):
    __tablename__ = "users"
    __allow_unmapped__ = True

    id: Mapped[int] = mapped_column(primary_key=True)
    email: Mapped[str] = mapped_column(String(255), nullable=False)
    password_hash: Mapped[str] = mapped_column(String(256), nullable=False)

    @classmethod
    def create(cls, email: str, raw_password: str) -> "User":
        return cls(email=email, password_hash=pwd_ctx.hash(raw_password))

    def verify_password(self, raw_password: str) -> bool:
        return pwd_ctx.verify(raw_password, self.password_hash)


@login_manager.user_loader
def load_user(user_id: str) -> User | None:
    return db.session.get(User, int(user_id))


def ensure_auth_schema() -> None:
    db.create_all()
    Base.metadata.create_all(bind=db.engine)
