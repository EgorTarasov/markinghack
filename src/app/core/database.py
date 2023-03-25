import time
from typing import Any

from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import OperationalError as sqlalchemyOpError
from psycopg2 import OperationalError as psycopg2OpError

from app.utils.logging import log
from app.core.settings import settings


engine: Engine
SessionLocal: Any

Base = declarative_base()


def connect_db():
    global engine
    global SessionLocal
    engine = create_engine(
        str(settings.DATABASE_URI),
        pool_pre_ping=True,
    )
    Base.metadata.bind = engine
    SessionLocal = sessionmaker(bind=engine)


def update_db():
    global Base
    Base.metadata.create_all(engine)


def init_db():
    connected = False
    while not connected:
        try:
            connect_db()
        except (sqlalchemyOpError, psycopg2OpError):
            log.info("failed to connect to db")
            time.sleep(2)
        else:
            connected = True
            update_db()
            log.info("initialized db")
    return SessionLocal
