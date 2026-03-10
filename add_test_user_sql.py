#!/usr/bin/env python3
"""
Simple script to add a test user directly to the existing SQLite database.
This bypasses all the Flask/SQLAlchemy complexity.
"""

import sqlite3
import os
from passlib.context import CryptContext

def add_test_user_directly():
    """Add test user directly to SQLite database"""
    
    # Test credentials
    test_email = os.environ.get("VOBCHAT_TEST_EMAIL", "VOBCHAT_TEST@email.com")
    test_password = os.environ.get("VOBCHAT_TEST_PASSWORD", "testpassword123")
    
    # Database path (use the one that has existing users)
    db_path = "/Users/xangma/Library/CloudStorage/OneDrive-Personal/repos/vobchat/src/instance/users.db"
    
    # Set up password hashing (same as in models.py)
    pwd_ctx = CryptContext(schemes=["argon2"], deprecated="auto")
    
    try:
        # Connect to database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check if user already exists
        cursor.execute("SELECT email FROM users WHERE email = ?", (test_email,))
        existing_user = cursor.fetchone()
        
        if existing_user:
            print(f"Test user '{test_email}' already exists.")
            # Update password
            password_hash = pwd_ctx.hash(test_password)
            cursor.execute("UPDATE users SET password_hash = ? WHERE email = ?", 
                         (password_hash, test_email))
            conn.commit()
            print(f"✓ Updated password for existing user '{test_email}'")
        else:
            # Create new user
            password_hash = pwd_ctx.hash(test_password)
            cursor.execute("INSERT INTO users (email, password_hash) VALUES (?, ?)", 
                         (test_email, password_hash))
            conn.commit()
            print(f"✓ Created new test user '{test_email}'")
        
        print("Test credentials:")
        print(f"  Email: {test_email}")
        print(f"  Password: {test_password}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"✗ Error: {e}")
        return False

if __name__ == "__main__":
    add_test_user_directly()