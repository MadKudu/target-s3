from datetime import datetime
from typing import Any

# Optional BSON support for MongoDB ObjectId serialization
# BSON is not always available, so we gracefully handle the import failure
try:
    from bson import ObjectId
    HAS_BSON = True
except ImportError:
    # If BSON is not installed, ObjectId will be None and HAS_BSON will be False
    # This allows the code to work without BSON dependency
    ObjectId = None
    HAS_BSON = False

# Use simplejson instead of standard json for better performance and additional features
# simplejson provides more control over serialization and handles edge cases better
from simplejson import JSONEncoder, dumps

from target_s3.formats.format_base import FormatBase


class JsonSerialize(JSONEncoder):
    def default(self, obj: Any) -> Any:
        if HAS_BSON and ObjectId is not None and isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        else:
            raise TypeError(f"Type {type(obj)} not serializable")


class FormatJsonl(FormatBase):
    def __init__(self, config, context) -> None:
        super().__init__(config, context, "jsonl")
        pass

    def _prepare_records(self):
        # use default behavior, no additional prep needed
        return super()._prepare_records()

    def _write(self) -> None:
        return super()._write('\n'.join(map(lambda record: dumps(record, cls=JsonSerialize), self.records)))

    def run(self) -> None:
        # use default behavior, no additional run steps needed
        return super().run(self.context["records"])
