# models.py — replace Dataset and Category classes

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean, func, Index, UniqueConstraint
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from database import Base
from datetime import datetime
import pytz
from passlib.context import CryptContext

IST = pytz.timezone("Asia/Kolkata")

def ist_now():
    return datetime.now(IST)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# ================= USER =================

class User(Base):
    __tablename__ = "users"

    id            = Column(Integer, primary_key=True, index=True)
    username      = Column(String, unique=True, nullable=False)
    email         = Column(String, unique=True, nullable=False)
    password      = Column(String, nullable=False)
    role          = Column(String, default="user")   # "user" | "admin"
    full_name     = Column(String)
    is_active     = Column(Boolean, default=True)
    created_at    = Column(DateTime(timezone=True), default=func.now())
    last_login    = Column(DateTime(timezone=True))

    # relationships (lazy so no auto-joins)
    datasets   = relationship("Dataset",  back_populates="owner",    cascade="all, delete-orphan")
    categories = relationship("Category", back_populates="owner",    cascade="all, delete-orphan")

    @staticmethod
    def hash_password(password: str) -> str:
        if not password:
            raise ValueError("Password is empty")
        if len(password.encode("utf-8")) > 72:
            password = password.encode("utf-8")[:72].decode("utf-8", errors="ignore")
        return pwd_context.hash(password)

    def verify_password(self, password: str) -> bool:
        return pwd_context.verify(password, self.password)

    def is_admin(self):
        return self.role == "admin"


# ================= CATEGORY =================

class Category(Base):
    __tablename__ = "categories"

    id      = Column(Integer, primary_key=True, index=True)
    name    = Column(String, nullable=False)          # ← removed unique=True
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)

    owner    = relationship("User", back_populates="categories")
    datasets = relationship("Dataset", back_populates="category_rel",
                            foreign_keys="Dataset.category_id")

    # Two users CAN have the same category name — uniqueness is per-user only
    __table_args__ = (
        UniqueConstraint("name", "user_id", name="uq_category_name_per_user"),
        Index("ix_category_user_id", "user_id"),
    )

    def __repr__(self):
        return f"<Category {self.name}>"


# ================= DATASET =================

class Dataset(Base):
    __tablename__ = "datasets"

    id               = Column(Integer, primary_key=True, index=True)
    file_name        = Column(String, nullable=False)
    file_path        = Column(String, nullable=False)

    # Keep 'department' for backward compat — category_id is the FK going forward
    department       = Column(String, nullable=False)
    description      = Column(String, nullable=True)

    row_count        = Column(Integer, default=0)
    actual_records   = Column(Integer, default=0)
    duplicate_records= Column(Integer, default=0)

    uploaded_at      = Column(DateTime, default=ist_now)

    # ── NEW COLUMNS ──────────────────────────────────────────
    user_id     = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"),  nullable=False)
    category_id = Column(Integer, ForeignKey("categories.id", ondelete="SET NULL"), nullable=True)
    # ─────────────────────────────────────────────────────────

    owner        = relationship("User",     back_populates="datasets")
    category_rel = relationship("Category", back_populates="datasets",
                                foreign_keys=[category_id])

    __table_args__ = (
        Index("ix_dataset_user_id", "user_id"),
    )

    @hybrid_property
    def category(self):
        return self.department

    @category.setter
    def category(self, value):
        self.department = value


# ================= UPLOAD LOG =================

class UploadLog(Base):
    __tablename__ = "logs"

    id         = Column(Integer, primary_key=True, index=True)
    file_name  = Column(String, nullable=False)
    status     = Column(String, nullable=False)
    message    = Column(String, nullable=True)
    created_at = Column(DateTime, default=ist_now)


# ================= DUPLICATE RELATION =================

class DuplicateRelation(Base):
    __tablename__ = "duplicate_relations"

    id         = Column(Integer, primary_key=True, index=True)
    dataset_id = Column(Integer, ForeignKey("datasets.id", ondelete="CASCADE"))
    phone      = Column(String)
    email      = Column(String)
    user_names = Column(String)
    user_count = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)