from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional, Union


@dataclass
class SCMColumn:
    name: str
    type: Union[str, datetime] = str
    nullable: bool = True
    comment: Optional[str] = None
    primary_key: bool = False
    additional_kwargs: Dict = field(default_factory=dict)

    def __call__(self, **kwargs):
        new_instance = self.__class__(
            name=self.name,
            type=self.type,
            nullable=self.nullable,
            comment=self.comment,
            primary_key=self.primary_key,
            additional_kwargs=self.additional_kwargs,
        )
        for key, value in kwargs.items():
            if hasattr(new_instance, key):
                setattr(new_instance, key, value)
            else:
                new_instance.additional_kwargs[key] = value
        return new_instance
