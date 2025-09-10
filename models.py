from sqlalchemy import (
    Column,
    String,
    BigInteger,
    Boolean,
    JSON,
    ARRAY,
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Post(Base):
    __tablename__ = "Posts"

    Id = Column(String, primary_key=True, index=True)
    CreateAt = Column(BigInteger)
    UpdateAt = Column(BigInteger)
    DeleteAt = Column(BigInteger)
    UserId = Column(String)
    ChannelId = Column(String)
    RootId = Column(String, nullable=True)
    OriginalId = Column(String, nullable=True)
    Message = Column(String)
    Type = Column(String)
    Props = Column(JSON)
    Hashtags = Column(String, nullable=True)
    Filenames = Column(ARRAY(String))
    FileIds = Column(ARRAY(String))
    HasReactions = Column(Boolean)
    EditAt = Column(BigInteger)
    IsPinned = Column(Boolean)
    RemoteId = Column(String, nullable=True)
