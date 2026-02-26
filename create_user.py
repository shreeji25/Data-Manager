from database import SessionLocal
from models import User

db = SessionLocal()

user = User(
    username="admin",
    email="admin@gmail.com",
    password=User.hash_password("admin123"),
    role="admin",
)

db.add(user)
db.commit()

print("Admin user created")
