"""
auth.py — Authentification JWT (python-jose + passlib/bcrypt)

Stockage des utilisateurs : table PostgreSQL `users` (créée au démarrage).
Pourquoi Postgres plutôt qu'en mémoire ? Persistance entre restarts du container,
et /auth/register devient immédiatement utile pour les tests sans redéploiement.

Flux : POST /auth/token (form-data username+password) → JWT Bearer (HS256, 30 min).
Dépendance get_current_user() à injecter dans chaque route protégée via Depends().
Config JWT lue depuis auth_config.yaml — clé secrète jamais codée en dur.
"""

import logging
import os
import yaml
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import Token, UserCreate, UserORM

logger = logging.getLogger(__name__)

_CONFIG_DIR = Path(os.environ.get("CONFIG_DIR", Path(__file__).parent.parent / "config"))


def _load_auth_config() -> dict:
    with open(_CONFIG_DIR / "auth_config.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


_jwt_cfg = _load_auth_config()["jwt"]

SECRET_KEY:                  str = _jwt_cfg["secret_key"]
ALGORITHM:                   str = _jwt_cfg["algorithm"]
ACCESS_TOKEN_EXPIRE_MINUTES: int = _jwt_cfg["access_token_expire_minutes"]

pwd_context   = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/token")

router = APIRouter(prefix="/auth", tags=["Auth"])


# ─── Utilitaires JWT ──────────────────────────────────────────────────────────

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Génère un JWT signé avec expiration (défaut : ACCESS_TOKEN_EXPIRE_MINUTES)."""
    payload = data.copy()
    expire  = datetime.now(timezone.utc) + (
        expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    payload["exp"] = expire
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def _hash_password(password: str) -> str:
    return pwd_context.hash(password)


def _verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


# ─── Dépendance FastAPI ───────────────────────────────────────────────────────

def get_current_user(
    token: str     = Depends(oauth2_scheme),
    db:    Session = Depends(get_db),
) -> UserORM:
    """
    Valide le JWT Bearer et retourne l'utilisateur courant.
    Lève HTTP 401 si le token est absent, invalide ou expiré.
    À injecter via Depends(get_current_user) dans chaque route protégée.
    """
    exc = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Token invalide ou expiré — authentifiez-vous via POST /auth/token",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload  = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if username is None:
            raise exc
    except JWTError:
        raise exc

    user = db.query(UserORM).filter(UserORM.username == username).first()
    if user is None:
        raise exc
    return user


# ─── Endpoints ────────────────────────────────────────────────────────────────

@router.post(
    "/token",
    response_model=Token,
    summary="Obtenir un token JWT (OAuth2 Password Flow)",
)
def login(
    form: OAuth2PasswordRequestForm = Depends(),
    db:   Session                   = Depends(get_db),
):
    """
    Envoyer `username` + `password` en **form-data** → retourne un JWT Bearer valable 30 min.

    Exemple curl :
    ```
    curl -X POST http://localhost:8000/auth/token \\
         -d "username=admin&password=admin123"
    ```
    """
    user = db.query(UserORM).filter(UserORM.username == form.username).first()
    if not user or not _verify_password(form.password, user.hashed_password):
        logger.warning("Échec de connexion pour : %s", form.username)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Identifiants incorrects",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = create_access_token({"sub": user.username})
    logger.info("Connexion réussie : %s", user.username)
    return Token(access_token=token)


@router.post(
    "/register",
    status_code=status.HTTP_201_CREATED,
    summary="Créer un compte utilisateur",
)
def register(user_in: UserCreate, db: Session = Depends(get_db)):
    """
    Crée un utilisateur avec mot de passe hashé en bcrypt.
    Envoyer un JSON : `{"username": "...", "password": "..."}`.
    """
    if db.query(UserORM).filter(UserORM.username == user_in.username).first():
        raise HTTPException(status_code=409, detail="Nom d'utilisateur déjà utilisé")
    db.add(UserORM(
        username=user_in.username,
        hashed_password=_hash_password(user_in.password),
    ))
    db.commit()
    logger.info("Nouvel utilisateur créé : %s", user_in.username)
    return {"message": f"Utilisateur '{user_in.username}' créé avec succès"}
