from config import Config
from sqlalchemy import MetaData, Table, and_, create_engine, or_, select
from sqlalchemy.orm import Session

engine = create_engine(Config.DATABASE_URI)
metadata = MetaData()

mapping_hotel = Table(
    "mapping_hotel",
    metadata,
    autoload_with=engine
)

mapping_hotel_room = Table(
    "mapping_hotel_room",
    metadata,
    autoload_with=engine
)

clients_operator = Table(
    "clients_operator",
    metadata,
    autoload_with=engine
)

accommodation_hotel_room = Table(
    "accommodation_hotel_room",
    metadata,
    autoload_with=engine,
)

reservations_booking = Table(
    "reservations_booking",
    metadata,
    autoload_with=engine,
)

definitions_meal_plan = Table(
    "definitions_meal_plan",
    metadata,
    autoload_with=engine,
)

class Query:
    @classmethod
    def get_hotel_info(cls, id):
        with Session(engine) as session:
            stmt = select(
                mapping_hotel.c.hotel_id,
                mapping_hotel.c.is_charter,
            ).where(
                mapping_hotel.c.id == id,
            )

            result = session.execute(stmt).fetchone()

            if result is None:
                return None
            return result

    @classmethod
    def get_room_id(cls, row):
        with Session(engine) as session:
            stmt = (
                select(accommodation_hotel_room.c.id)
                .join_from(mapping_hotel_room, accommodation_hotel_room)
                .where(
                    or_(
                        and_(
                            mapping_hotel_room.c.room_code == row["room_code"],
                            accommodation_hotel_room.c.hotel_id == row["hotel_id_y"],
                        ),
                        and_(
                            accommodation_hotel_room.c.name == row["room_type"],
                            accommodation_hotel_room.c.hotel_id == row["hotel_id_y"],
                        ),
                    )
                )
            )

            result = session.scalar(stmt)

            if result is None:
                return None
            return result
