from typing import List, Dict, Any, Optional
from datetime import datetime
from pydantic import BaseModel, Field



class FuturesOIContract(BaseModel):
    商品名稱: str
    身份別: str
    多方交易口數: int
    多方交易金額: int
    空方交易口數: int
    空方交易金額: int
    交易淨口數: int
    交易淨金額: int
    多方未平倉口數: int
    多方未平倉金額: int
    空方未平倉口數: int
    空方未平倉金額: int
    未平倉淨口數: int
    未平倉淨金額: int

class DailyFuturesData(BaseModel):
    id: str = Field(alias='_id')
    日期: datetime
    期貨契約: List[FuturesOIContract]