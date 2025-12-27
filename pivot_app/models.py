from dataclasses import dataclass
from typing import Optional


@dataclass
class DataSource:
    kind: str  # "path" or "upload"
    path: Optional[str] = None
    uploaded_bytes: Optional[bytes] = None
    name: Optional[str] = None
