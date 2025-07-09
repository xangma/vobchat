#!/usr/bin/env python3
"""
Simple script to create a user in VobChat without interactive prompts
Usage: python create_user.py email@example.com password123
"""
import sys
import os
sys.path.insert(0, '/app/src')

from vobchat.models import db, User, pwd_ctx
from vobchat.app import create_app

def create_user(email, password):
    """Create a user with the given email and password"""
    # Create Flask app context
    app = create_app()
    
    with app.server.app_context():
        email = email.strip().lower()
        
        # Check if user already exists
        user = db.session.scalar(db.select(User).filter_by(email=email))
        if user:
            # Reset password for existing user
            user.password_hash = pwd_ctx.hash(password)
            verb = "Password reset for"
        else:
            # Create new user
            user = User.create(email, password)
            db.session.add(user)
            verb = "Created user"
        
        try:
            db.session.commit()
            print(f"✓ {verb}: {email}")
            return True
        except Exception as e:
            db.session.rollback()
            print(f"✗ Error: {e}")
            return False

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python create_user.py email@example.com password123")
        sys.exit(1)
    
    email = sys.argv[1]
    password = sys.argv[2]
    
    success = create_user(email, password)
    sys.exit(0 if success else 1)