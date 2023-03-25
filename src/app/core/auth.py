from datetime import timedelta, datetime
from typing import Optional


from fastapi import HTTPException, Depends, status

from sqlalchemy.orm import Session

from jose import JWTError, jwt
from passlib.context import CryptContext

from app.core import crud
from app.core.settings import settings
from app.core.schemas import Token, TokenData
from app.core.models import User
from app.utils.logging import log
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/token")


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    hashed_password = pwd_context.hash(password)
    return hashed_password


def authenticate_user(db: Session, username: str, password: str) -> User | bool:
    """Авторизация пользователя

    Авторизация в <service name>

    Args:
        db (Session): сессия sqlalchemy.orm
        username (str):
        password (str):

    Returns:
        CustomUser | bool:
    """
    user = crud.get_user_by_username(db, username)

    if user and not verify_password(password, user.password):
        return False
    return user


def sign_user(db: Session, username: str, password: str) -> User:
    return User()


def create_access_token(
    data: dict[str, str | datetime], expires_delta: timedelta | None = None
) -> str:
    """Creating JWT token

    Creating JWT token for LMS backend service

    Args:
        data (dict): with format : {"sub": username : str}
        expires_delta (timedelta | None, optional): time in which token will expire.

    Returns:
        str: JWT token string
    """
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(
            minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES
        )
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(
        to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM
    )
    return encoded_jwt


def get_current_user(db: Session, token: str = Depends(oauth2_scheme)) -> User:

    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=settings.ALGORITHM)
        username: Optional[str] = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception

    user = crud.get_user_by_username(db, token_data.username)  # type: ignore

    if user is None:
        raise credentials_exception
    return user
