from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String

Base = declarative_base()

class Proteina(Base):
    __tablename__ = 'proteinas'
    id = Column(Integer, primary_key=True)
    proteina_id = Column(String, unique=True)
