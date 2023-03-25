from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.core.dependencies import get_db
from app.core.auth import get_password_hash, oauth2_scheme, get_current_user
from app.core.crud import save_user
from app.core.schemas import UserSchema, UserCreateSchema

router = APIRouter(
    prefix="/user",
    tags=["user"],
    responses={404: {"description": "Not found"}},
)


@router.post("/create_user", response_model=UserSchema)
async def create_user(
    user: UserCreateSchema, db: Session = Depends(get_db)
) -> UserSchema:
    user.password = get_password_hash(user.password)
    user = save_user(db, user.username, user.password, user.email)
    return UserSchema.from_orm(user)


@router.get("/me", response_model=UserSchema)
async def profile(
    token=Depends(oauth2_scheme), db: Session = Depends(get_db)
) -> UserSchema:
    user = get_current_user(db, token)
    return UserSchema.from_orm(user)
