from pydantic import BaseModel, Field
from typing import List, Dict, Optional


class Post(BaseModel):
    id: str = Field(..., alias='Id')
    create_at: int = Field(..., alias='CreateAt')
    update_at: int = Field(..., alias='UpdateAt')
    delete_at: int = Field(..., alias='DeleteAt')
    user_id: str = Field(..., alias='UserId')
    channel_id: str = Field(..., alias='ChannelId')
    root_id: Optional[str] = Field(None, alias='RootId')
    original_id: Optional[str] = Field(None, alias='OriginalId')
    message: str = Field(..., alias='Message')
    type: str = Field(..., alias='Type')
    props: Dict = Field(..., alias='Props')
    hashtags: Optional[str] = Field(None, alias='Hashtags')
    filenames: List[str] = Field(..., alias='Filenames')
    file_ids: List[str] = Field(..., alias='FileIds')
    has_reactions: bool = Field(..., alias='HasReactions')
    edit_at: int = Field(..., alias='EditAt')
    is_pinned: bool = Field(..., alias='IsPinned')
    remote_id: Optional[str] = Field(None, alias='RemoteId')

    class Config:
        orm_mode = True
        allow_population_by_field_name = True
