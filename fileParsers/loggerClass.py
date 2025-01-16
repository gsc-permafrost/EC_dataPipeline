# Generic template for every logger object
from dataclasses import dataclass,field

@dataclass
class Logger:
    type: str = None
    siteID: str = None
    positionID: int = 1
    descritption: str = None
    xyCoords: list = field(default_factory=lambda:[None,None])