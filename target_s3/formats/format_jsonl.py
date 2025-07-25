from datetime import datetime
import math
import json

from bson import ObjectId

from target_s3.formats.format_base import FormatBase


class JsonSerialize(json.JSONEncoder):
    def default(self, obj: any) -> any:
        if isinstance(obj, ObjectId):
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
        return super()._write('\n'.join(json.dumps(r, cls=JsonSerialize) for r in self.records))

    def run(self) -> None:
        # use default behavior, no additional run steps needed
        return super().run(self.context["records"])
