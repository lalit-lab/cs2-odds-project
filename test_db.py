import bcrypt
from app.database import SessionLocal, User

def hash_password(password: str) -> str:
    # 1. Convert password to bytes
    pwd_bytes = password.encode('utf-8')
    # 2. Generate a salt and hash it
    salt = bcrypt.gensalt()
    hashed_password = bcrypt.hashpw(pwd_bytes, salt)
    # 3. Return as a string to store in DB
    return hashed_password.decode('utf-8')

print("=" * 60)
print("TESTING DATABASE CONNECTION (MODERN BCRYPT)")
print("=" * 60)

db = SessionLocal()

try:
    print("\n1. Cleaning up and Creating test user...")
    
    # Remove old user if exists
    existing_user = db.query(User).filter(User.username == "lalit").first()
    if existing_user:
        db.delete(existing_user)
        db.commit()

    # Create new user using the NEW hashing method
    test_user = User(
        username="lalit",
        email="lalit@example.com",
        password_hash=hash_password("test123"), # Using the new function
        virtual_balance=10000.0
    )
    
    db.add(test_user)
    db.commit()
    db.refresh(test_user)
    
    print(f"    ✅ User created successfully!")
    print(f"    - Username: {test_user.username}")
    print(f"    - Password Hash: {test_user.password_hash[:20]}...")
    
    print("\n2. Verifying database query...")
    user = db.query(User).filter(User.username == "lalit").first()
    if user:
        print(f"    ✅ Database query successful!")
    
    print("\n" + "=" * 60)
    print("🎉 SUCCESS: DATABASE & HASHING WORKING!")
    print("=" * 60)
    
except Exception as e:
    print(f"\n    ❌ Critical Error: {e}")
    db.rollback()
finally:
    db.close()