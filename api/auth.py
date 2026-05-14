"""
auth.py — Authentification JWT

Rôle  : Générer et valider les tokens JWT, fournir la dépendance
         FastAPI get_current_user() utilisée par tous les endpoints protégés.

TODO Phase 5 :
    - Implémenter create_access_token() avec python-jose
    - Implémenter verify_password() et get_password_hash() avec passlib
    - Implémenter get_current_user() comme dépendance FastAPI
    - Endpoint POST /auth/token (login → retourne JWT)
"""

# from datetime import datetime, timedelta
# from jose import JWTError, jwt
# from passlib.context import CryptContext
# from fastapi import Depends, HTTPException, status
# from fastapi.security import OAuth2PasswordBearer

SECRET_KEY = "changeme-in-production"   # TODO : charger depuis variable d'environnement
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30


def create_access_token(data: dict) -> str:
    """Génère un token JWT signé. TODO Phase 5."""
    pass


def verify_token(token: str) -> dict:
    """Valide un token JWT et retourne le payload. TODO Phase 5."""
    pass


def get_current_user():
    """Dépendance FastAPI : extrait et valide l'utilisateur depuis le header Authorization. TODO Phase 5."""
    pass
