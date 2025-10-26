class DataOperation:
    def __init__(self, connection, filters=None):
        self.connection = connection
        self.client = connection.session      # supabase client
        self.table  = connection.table        # table name
        self.local_dir = connection.local_dir
        self.filters = filters or {}

    def push(self, entry):
        # 1) prepare payload
        # record = Serializer.to_db(entry)   | We might not actually really need to use Serializer.
        record = entry if isinstance(entry, dict) else entry.__dict__

        # 2) always save locally first (durability)
        try:
            local_path = LocalCache.save(self.local_dir, record)
        except Exception as e:
            # if local save fails, abort — we can't guarantee data durability
            entry.meta['sync_status'] = 'local_save_failed'
            entry.meta['error'] = str(e)
            return {"ok": False, "reason": "local_save_failed", "error": str(e)}

        # 3) try remote insert (best-effort)
        try:
            # supabase-py style: client.table(table_name).insert(payload).execute()
            resp = self.client.table(self.stack).insert(record).execute()
            # inspect resp for errors or returned rows
            if getattr(resp, "error", None):
                # mark pending, keep local copy
                entry.meta['sync_status'] = 'pending'
                entry.meta['remote_error'] = str(resp.error)
                return {"ok": False, "reason": "remote_error", "resp": resp}
            else:
                entry.meta['sync_status'] = 'synced'
                entry.meta['remote_id'] = record.get("_id")
                # optionally update local copy (mark synced)
                LocalCache.save(self.local_dir, record)  # overwrite with new meta
                return {"ok": True, "resp": resp}
        except Exception as exc:
            # network or SDK error: keep local copy and mark pending
            entry.meta['sync_status'] = 'pending'
            entry.meta['remote_error'] = str(exc)
            return {"ok": False, "reason": "exception", "error": str(exc)}

    def pull(self, filters=None, limit=100):
        # Try remote first (fresh), fallback to local if remote fails.
        filters = filters or self.conn.default_filters or {}
        try:
            query = self.client.table(self.stack).select("*")
            # apply simple filters (example: eq)
            for k, v in filters.items():
                query = query.eq(k, v)
            if limit:
                query = query.limit(limit)
            resp = query.execute()
            if getattr(resp, "error", None):
                raise RuntimeError(resp.error)
            rows = resp.data if hasattr(resp, "data") else resp
            # transform to Entry objects
            return [Serializer.from_db(r) for r in rows]
        except Exception as e:
            # remote failed — return local matches instead
            local_rows = LocalCache.load(self.local_dir, filters=filters)
            return [Serializer.from_db(r) for r in local_rows]