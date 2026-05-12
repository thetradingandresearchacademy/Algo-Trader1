# FastAPI Route Example
from fastapi import APIRouter
from pydantic import BaseModel
from services.redis_stream import RedisStream

router = APIRouter()
redis = RedisStream()

class ModeUpdate(BaseModel):
    trading_mode: str  # "INDEX", "STOCK", or "BOTH"

@router.post("/api/system/mode")
async def update_trading_mode(payload: ModeUpdate):
    command_data = {
        "command": "UPDATE_MODE",
        "new_mode": payload.trading_mode,
        "timestamp": time.time()
    }
    # Publish instantly to the command bus
    await redis.publish("control_commands", command_data)
    return {"status": "success", "mode": payload.trading_mode}