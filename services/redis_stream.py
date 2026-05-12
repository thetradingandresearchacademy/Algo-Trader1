import redis
import json
from config.settings import REDIS_HOST, REDIS_PORT


class RedisStream:
    """
    Redis Stream wrapper used as the event bus for the Algo OS.

    Responsibilities:
    - Publish events to Redis streams
    - Read events from Redis streams
    - Provide safe error handling
    - Helper for daily context IDs
    """

    def __init__(self):

        try:

            self.client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5
            )

            # Test connection
            self.client.ping()

            print("Redis connection established")

        except Exception as e:

            print("Redis connection failed:", e)
            raise

    def get_today_id(self):
        """Returns the Redis stream ID for the start of today in IST (00:00:00)."""
        from datetime import datetime, timedelta, timezone
        ist = timezone(timedelta(hours=5, minutes=30))
        today_start = datetime.now(ist).replace(hour=0, minute=0, second=0, microsecond=0)
        # Redis stream IDs are in milliseconds
        return f"{int(today_start.timestamp() * 1000)}-0"

    def get_latest_id(self, stream):
        """Returns the latest message ID in the stream to prevent reading historical data."""
        try:
            info = self.client.xinfo_stream(stream)
            return info.get("last-generated-id", "0-0")
        except Exception:
            return "0-0"

    # -----------------------------------------------------
    # Publish Event
    # -----------------------------------------------------

    async def publish(self, stream, payload):

        try:

            message = {
                "data": json.dumps(payload)
            }

            self.client.xadd(stream, message)

        except Exception as e:

            print(f"Redis publish error [{stream}]:", e)

    # -----------------------------------------------------
    # Read Stream
    # -----------------------------------------------------

    def read(self, stream, last_id="0-0", count=100):

        """
        Reads messages from a Redis stream.

        Returns:
            list of messages
        """

        try:

            messages = self.client.xread(
                {stream: last_id},
                count=count
            )

            return messages

        except Exception as e:

            print(f"Redis read error [{stream}]:", e)
            return []

    # -----------------------------------------------------
    # Hash Operations (for persistence)
    # -----------------------------------------------------

    def set_hash(self, name, key, value):
        """Set a field in a Redis hash."""
        try:
            if isinstance(value, (dict, list)):
                value = json.dumps(value)
            self.client.hset(name, key, value)
        except Exception as e:
            print(f"Redis hset error [{name}:{key}]: {e}")

    def get_hashall(self, name):
        """Get all fields in a Redis hash."""
        try:
            data = self.client.hgetall(name)
            # Parse JSON strings back to dicts if possible
            parsed = {}
            for k, v in data.items():
                try:
                    parsed[k] = json.loads(v)
                except:
                    parsed[k] = v
            return parsed
        except Exception as e:
            print(f"Redis hgetall error [{name}]: {e}")
            return {}

    def delete_hash(self, name, key):
        """Delete a field from a Redis hash."""
        try:
            self.client.hdel(name, key)
        except Exception as e:
            print(f"Redis hdel error [{name}:{key}]: {e}")

    # -----------------------------------------------------
    # Health Check
    # -----------------------------------------------------

    def health(self):

        try:

            return self.client.ping()

        except Exception:

            return False