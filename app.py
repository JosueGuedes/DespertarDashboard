"""
Backend com cache: consulta planilhas públicas do Google Sheets a cada
POLL_INTERVAL segundos e envia só as mudanças aos navegadores via SSE.

Rodar:  uvicorn app:app --reload
Abrir:  http://localhost:8000
"""
import asyncio
import csv
import io
import json
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from fastapi.responses import FileResponse, StreamingResponse

POLL_INTERVAL = 1  # segundos entre consultas ao Google

# Planilhas/intervalos que o site vai exibir.
# sheet_id: o trecho entre /d/ e /edit na URL da planilha
# gid: id da aba (aparece na URL como #gid=...)
SOURCES = [
    {"name": "Kazuma Mori", "sheet_id": "1HnwTbDihbB3DCGy6_XpexpY5CiY1hVcVYEWw1XauUA0", "gid": "748574273", "range": "W71:AA75"},
]

cache: dict[str, list[list[str]]] = {}   # último valor conhecido de cada fonte
subscribers: set[asyncio.Queue] = set()  # um Queue por navegador conectado


async def fetch_source(client: httpx.AsyncClient, src: dict) -> list[list[str]]:
    url = f"https://docs.google.com/spreadsheets/d/{src['sheet_id']}/gviz/tq"
    params = {"tqx": "out:csv", "gid": src["gid"], "range": src["range"]}
    resp = await client.get(url, params=params, follow_redirects=True)
    resp.raise_for_status()
    return list(csv.reader(io.StringIO(resp.text)))


def broadcast(message: dict) -> None:
    payload = json.dumps(message, ensure_ascii=False)
    for queue in subscribers:
        queue.put_nowait(payload)


async def poll_loop() -> None:
    async with httpx.AsyncClient(timeout=15) as client:
        while True:
            for src in SOURCES:
                try:
                    data = await fetch_source(client, src)
                except Exception as exc:  # mantém o último valor em caso de erro
                    print(f"[erro] {src['name']}: {exc}")
                    continue
                if cache.get(src["name"]) != data:  # só avisa se mudou
                    cache[src["name"]] = data
                    broadcast({"name": src["name"], "data": data})
            await asyncio.sleep(POLL_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(poll_loop())
    yield
    task.cancel()


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def index():
    return FileResponse("index.html")


@app.get("/api/data")
async def snapshot():
    """Estado atual completo (útil para testes ou clientes sem SSE)."""
    return cache


@app.get("/events")
async def events():
    queue: asyncio.Queue = asyncio.Queue()

    async def stream():
        subscribers.add(queue)
        try:
            # Ao conectar, envia tudo o que já está em cache
            for name, data in cache.items():
                yield f"data: {json.dumps({'name': name, 'data': data}, ensure_ascii=False)}\n\n"
            while True:
                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=25)
                    yield f"data: {payload}\n\n"
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"  # evita que proxies fechem a conexão
        finally:
            subscribers.discard(queue)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )