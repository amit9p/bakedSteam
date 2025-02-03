Typing/Dataclasses

If you’re using Python 3.7+, you can leverage dataclasses or TypedDict to give structure. This can help with IDE autocompletion and catch mistakes earlier.

Example with a simple dataclass for columns:

from dataclasses import dataclass

@dataclass(frozen=True)
class Schema:
    user_id: str = "user_id"
    order_id: str = "order_id"
    timestamp: str = "timestamp"

Then reference Schema().user_id in your code.

