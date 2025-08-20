import os, json, uuid
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timezone

def now_utc():
    return datetime.now(timezone.utc)

class JsonStorage:
    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        if not os.path.exists(self.path):
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump({"links": []}, f)

    def _read(self):
        with open(self.path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _write(self, data):
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)

    def list_links(self, limit: int, offset: int, tag: Optional[str], q: Optional[str]):
        data = self._read()
        items = data.get("links", [])
        if tag:
            items = [i for i in items if tag in (i.get("tags") or [])]
        if q:
            ql = q.lower()
            def matches(i):
                return any([
                    ql in (i.get("title","") or "").lower(),
                    ql in (i.get("url","") or "").lower(),
                    ql in (i.get("notes","") or "").lower(),
                ])
            items = [i for i in items if matches(i)]
        items.sort(key=lambda x: x.get("updated_at",""), reverse=True)
        total = len(items)
        return items[offset:offset+limit], total

    def create_link(self, item: Dict[str, Any]):
        data = self._read()
        item["id"] = str(uuid.uuid4())
        now = now_utc()
        item["created_at"] = now
        item["updated_at"] = now
        data["links"].append(item)
        self._write(data)
        return item

    def create_links_bulk(self, items: List[Dict[str, Any]]):
        data = self._read()
        now = now_utc()
        out = []
        for it in items:
            rec = dict(it)
            rec["id"] = str(uuid.uuid4())
            rec["created_at"] = now
            rec["updated_at"] = now
            data["links"].append(rec)
            out.append(rec)
        self._write(data)
        return out

    def get_link(self, link_id: str):
        data = self._read()
        for i in data.get("links", []):
            if i["id"] == link_id:
                return i
        return None

    def update_link(self, link_id: str, patch: Dict[str, Any]):
        data = self._read()
        for i in data.get("links", []):
            if i["id"] == link_id:
                i.update({k:v for k,v in patch.items() if v is not None})
                i["updated_at"] = now_utc()
                self._write(data)
                return i
        return None

    def delete_link(self, link_id: str):
        data = self._read()
        before = len(data.get("links", []))
        data["links"] = [i for i in data.get("links", []) if i["id"] != link_id]
        after = len(data["links"])
        self._write(data)
        return after < before

    def export_all(self):
        data = self._read()
        return data.get("links", [])

def get_storage():
    if os.getenv("DATABASE_URL"):
        print("[storage] Usando Postgres (DATABASE_URL detectada)")
        from storage_pg import PgStorage
        return PgStorage()
    print("[storage] Usando JSON local (sin DATABASE_URL)")
    path = os.getenv("DATA_FILE", "./data/links.json")
    return JsonStorage(path)

