from dataclasses import dataclass
from typing import Optional
from uuid import UUID


@dataclass
class DeviceStatus:
    online: bool
    status: str


@dataclass
class DeviceInfo:
    uuid: UUID
    server_uuid: UUID
    name: str
    type: int
    address: str
    port: int
    available: bool = True
    user: Optional[int] = None
    databaseId: Optional[int] = None
    dataHandlerActive: Optional[bool] = True
