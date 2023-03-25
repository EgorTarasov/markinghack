from fastapi import APIRouter
from app.endpoints.auth import router as auth_router
from app.endpoints.user import router as user_router
from app.endpoints.item import router as item_router
from app.endpoints.ml import router as ml_router

router = APIRouter(
    responses={404: {"description": "Not found"}},
)


router.include_router(auth_router)
router.include_router(user_router)
router.include_router(item_router)
router.include_router(ml_router)
