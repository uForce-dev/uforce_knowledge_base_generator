from sqlalchemy import (
    Column,
    String,
    BigInteger,
    Boolean,
    JSON,
    Text,
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Post(Base):
    __tablename__ = "Posts"

    Id = Column(String(26), primary_key=True, index=True)
    CreateAt = Column(BigInteger)
    UpdateAt = Column(BigInteger)
    DeleteAt = Column(BigInteger)
    UserId = Column(String(26))
    ChannelId = Column(String(26))
    RootId = Column(String(26), nullable=True)
    OriginalId = Column(String(26), nullable=True)
    Message = Column(Text)
    Type = Column(String(50))
    Props = Column(JSON)
    Hashtags = Column(Text, nullable=True)
    Filenames = Column(JSON)
    FileIds = Column(JSON)
    HasReactions = Column(Boolean)
    EditAt = Column(BigInteger)
    IsPinned = Column(Boolean)
    RemoteId = Column(String(26), nullable=True)


class User(Base):
    __tablename__ = "Users"

    Id = Column(String(26), primary_key=True, index=True)
    Username = Column(String(64), nullable=False)


class Channel(Base):
    __tablename__ = "Channels"

    Id = Column(String(26), primary_key=True, index=True)
    Type = Column(String(1), nullable=False)
    DisplayName = Column(String(64), nullable=False)
    Name = Column(String(64), nullable=False)
