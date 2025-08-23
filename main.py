from dotenv import load_dotenv
load_dotenv()

import httpx
import os, io, csv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from typing import Optional, List
from models import LinkIn, Link, LinkUpdate
from storage import get_storage
from datetime import datetime
from pydantic import BaseModel
from fastapi.staticfiles import StaticFiles

CORS_ORIGINS_ENV = os.getenv("CORS_ORIGINS", "*")

app = FastAPI(
    title="Links API",
    version="1.2.0",
    description="API para guardar y consultar enlaces (Neon Postgres & UI)."
)

origins = [o.strip() for o in CORS_ORIGINS_ENV.split(",")] if CORS_ORIGINS_ENV else ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

storage = get_storage()


class LinksResponse(BaseModel):
    links: List[Link]
    total: int


@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat() + "Z"}


@app.get("/links", response_model=LinksResponse)
def list_links(limit: int = 100, offset: int = 0, tag: Optional[str] = None, q: Optional[str] = None):
    items, total = storage.list_links(limit=limit, offset=offset, tag=tag, q=q)
    return {"links": items, "total": total}


@app.post("/links", response_model=Link)
def create_link(payload: LinkIn):
    # Convertir a tipos JSON-serializables (AnyHttpUrl -> str)
    created = storage.create_link(payload.model_dump(mode="json"))
    return created


@app.post("/links/bulk", response_model=LinksResponse)
def create_links_bulk(payload: List[LinkIn]):
    created = storage.create_links_bulk([p.model_dump(mode="json") for p in payload])
    return {"links": created, "total": len(created)}


@app.get("/links/{link_id}", response_model=Link)
def get_link(link_id: str):
    found = storage.get_link(link_id)
    if not found:
        raise HTTPException(status_code=404, detail="Link not found")
    return found


@app.put("/links/{link_id}", response_model=Link)
def update_link(link_id: str, patch: LinkUpdate):
    # No enviar claves con None para no sobreescribir en DB
    updated = storage.update_link(link_id, patch.model_dump(mode="json", exclude_none=True))
    if not updated:
        raise HTTPException(status_code=404, detail="Link not found")
    return updated


@app.delete("/links/{link_id}")
def delete_link(link_id: str):
    ok = storage.delete_link(link_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Link not found")
    return {"deleted": True}


@app.get("/export.json")
def export_all_json():
    items = storage.export_all()
    return {"links": items}


@app.get("/export.csv", response_class=PlainTextResponse)
def export_all_csv():
    items = storage.export_all()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "url", "title", "tags", "notes", "created_at", "updated_at"])
    for i in items:
        tags = ",".join(i.get("tags", []))
        writer.writerow([
            i.get("id", ""),
            i.get("url", ""),
            i.get("title", ""),
            tags,
            i.get("notes", ""),
            i.get("created_at", ""),
            i.get("updated_at", "")
        ])
    return output.getvalue()

@app.get("/search_google")
def search_google(q: str, num: int = 5):
    """
    Busca en Google Programmable Search (Custom Search JSON API)
    usando GOOGLE_API_KEY y GOOGLE_CX del .env.
    """
    api_key = os.getenv("GOOGLE_API_KEY")
    cx = os.getenv("GOOGLE_CX")

    if not api_key or not cx:
        raise HTTPException(status_code=500, detail="Google API Key o CX no configurados")

    url = "https://www.googleapis.com/customsearch/v1"
    params = {"q": q, "key": api_key, "cx": cx, "num": num, "hl": "es", "safe": "active"}

    try:
        r = httpx.get(url, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        results = [
            {
                "title": it.get("title"),
                "link": it.get("link"),
                "snippet": it.get("snippet"),
                "displayLink": it.get("displayLink"),
            }
            for it in data.get("items", [])[:num]
        ]
        return {"query": q, "results": results}
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Error consultando Google: {e}")

# Montar UI estática
app.mount("/", StaticFiles(directory="static", html=True), name="static")
