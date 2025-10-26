import json, os, time, uuid

class LocalCache:
    @staticmethod
    def _path(base_dir, record_id=None):
        if record_id is None:
            record_id = str(uuid.uuid4())
        filename = f"{record_id}.json"
        return os.path.join(base_dir, filename)

    @staticmethod
    def save(base_dir, record):
        os.makedirs(base_dir, exist_ok=True)
        record_id = record.get("id") or str(uuid.uuid4())
        record["id"] = record_id
        record["local_saved_at"] = time.time()
        path = LocalCache._path(base_dir, record_id)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        return path

    @staticmethod
    def load(base_dir, filters=None):
        filters = filters or {}
        if not os.path.exists(base_dir):
            return []
        results = []
        for file in os.listdir(base_dir):
            if file.endswith(".json"):
                path = os.path.join(base_dir, file)
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    # crude filter: all filter keys must match exactly
                    if all(data.get(k) == v for k, v in filters.items()):
                        results.append(data)
                except Exception:
                    continue
        return results
