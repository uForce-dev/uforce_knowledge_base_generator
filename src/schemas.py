from pydantic import ConfigDict, Field, BaseModel


class TeamlyArticle(BaseModel):
    id: str
    title: str
    type: str
    is_archived: bool = Field(alias="isArchived")
    parent_space_id: str | None = Field(default=None, alias="parentSpaceId")
    created_at: str | None = Field(default=None, alias="createdAt")
    updated_at: str | None = Field(default=None, alias="updatedAt")
    created_by: str | None = Field(default=None, alias="createdBy")

    model_config = ConfigDict(populate_by_name=True, extra="ignore")
