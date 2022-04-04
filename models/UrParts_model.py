from pydantic import BaseModel
from typing import Optional


class UrPartsModel(BaseModel):
    make: Optional[str]
    category: Optional[str]
    model: Optional[str]
    part: Optional[str]
