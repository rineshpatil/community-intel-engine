from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


class NormalizedItem(BaseModel):
    item_id: str = ""
    source: Literal["github", "reddit"]
    source_kind: str
    external_id: str
    # Parent issue for a comment; parent submission for a reddit comment.
    parent_external_id: str | None = None
    channel: str
    url: str
    author: str          # username only, never an email address
    title: str | None = None
    body: str
    created_at: datetime
    ingested_at: datetime
    raw: dict[str, Any] = Field(default_factory=dict)

    @staticmethod
    def make_id(source: str, external_id: str) -> str:
        return f"{source}#{external_id}"

    @model_validator(mode="after")
    def _derive_item_id(self) -> "NormalizedItem":
        object.__setattr__(
            self, "item_id", self.make_id(self.source, self.external_id)
        )
        return self

    def to_dynamo_item(self) -> dict[str, Any]:
        # mode="json" converts datetimes to ISO strings, which DynamoDB accepts.
        data = self.model_dump(mode="json")
        data["pk"] = f"ITEM#{self.item_id}"
        data["sk"] = "META"
        return data
