'''
Simple implementation of single-request Redis getters for primitive types:
- With defaults.
- With autocomplete (types aren't passed as arguments).
- No camelCase.

Usage:

```
redis_client = get_redis_client()

redis_client.set('my_int', 42)
redis_client.set('my_float', 3.14)
redis_client.set('my_bool', 1)

# Get typed values with defaults:
default_val = redis_client.get_str('my_key', 'default_value')
int_val = redis_client.get_int('my_int', 0)
float_val = redis_client.get_float('my_float', 0.0)
bool_val = redis_client.get_bool('my_bool', False)
```
'''

import redis
from config import settings


class RedisClient(redis.Redis):
    BOOL_MAP = {
        'true': True,
        '1': True,
        'false': False,
        '0': False,
    }

    def _get_and_cast(self, key: str, type_func, default):
        # Add exception handling here, if failing fast in undesired
        val = super().get(key)
        if val is None:
            return default

        return type_func(val)

    def get_str(self, key: str, default: str | None = None) -> str | None:
        return self._get_and_cast(key, str, default)

    def get_int(self, key: str, default: int | None = None) -> int | None:
        return self._get_and_cast(key, int, default)
        
    def get_float(self, key: str, default: float | None = None) -> float | None:
        return self._get_and_cast(key, float, default)

    def get_bool(self, key: str, default: bool | None = None) -> bool | None:
        val = self.get(key)
        if val is not None:
            return self.BOOL_MAP.get(val.lower(), default)

        return default


def get_redis_client(**kwargs) -> RedisClient:
    params = {
        'url': settings.REDIS_URI,
        'encoding': 'utf-8',
        'decode_responses': True,
    }
    params.update(kwargs)

    return RedisClient.from_url(**params)
