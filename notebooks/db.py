from config import Config
from sqlalchemy import MetaData, Table, and_, create_engine, or_, select
from sqlalchemy.orm import Session

engine = create_engine(Config.DATABASE_URI)
metadata = MetaData()

mapping_hotel = Table(
    "mapping_hotel",
    metadata,
    autoload_with=engine,
)
