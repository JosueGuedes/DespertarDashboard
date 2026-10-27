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

POLL_INTERVAL = 3  # segundos entre ciclos (1 s com 11 ranges tende a gerar bloqueio 429 do Google)

# Campos organizados por grupo: {grupo: {rótulo exibido no site: range}}
RANGES = {
    "HP e PE": {
        "HP atual": "W71:AA75",
        "HP máximo": "AC73:AE75",
        "PE atual": "AQ71:AU75",
        "PE máximo": "AW73:AY75",
    },
    "Testes de resistência": {
        "Astúcia": "Q23:S23",
        "Fortitude": "Q25:S25",
        "Integridade": "Q27:S27",
        "Reflexos": "Q29:S29",
        "Vontade": "Q31:S31",
    },
    "Outros": {
        "CA": "Y13:AC15",  # Classe de armadura
        "Atenção": "AI47:AK47",
    },
}

# sheet_id: o trecho entre /d/ e /edit na URL da planilha
# gid: id da aba (aparece na URL como #gid=...)
SOURCES = [
    {"name": "Kazuma Mori", "sheet_id": "1HnwTbDihbB3DCGy6_XpexpY5CiY1hVcVYEWw1XauUA0", "gid": "748574273"},
    {"name": "Kenta", "sheet_id": "1RtlpWO4nAbsTonfhK1JkvF9nUD3bXPbN6PvSKAGcneU", "gid": "748574273"},
    {"name": "OR-1", "sheet_id": "1qVXItLQaXlxddT40AMPAavIsQ-PbgAJfbMqnq6wj_Q4", "gid": "748574273"},

]

cache: dict[str, dict] = {}              # chave "fonte|grupo|campo" -> última mensagem enviada
subscribers: set[asyncio.Queue] = set()  # um Queue por navegador conectado


def trim(rows: list[list[str]]) -> list[list[str]]:
    """Remove células vazias de cada linha (sobras de células mescladas) e linhas vazias."""
    limpas = [[c for c in row if c.strip()] for row in rows]
    return [row for row in limpas if row]


async def fetch_range(client: httpx.AsyncClient, src: dict, rng: str) -> list[list[str]]:
    url = f"https://docs.google.com/spreadsheets/d/{src['sheet_id']}/gviz/tq"
    params = {"tqx": "out:csv", "gid": src["gid"], "range": rng, "headers": "0"}
    resp = await client.get(url, params=params, follow_redirects=True)
    resp.raise_for_status()
    return trim(list(csv.reader(io.StringIO(resp.text))))


def broadcast(message: dict) -> None:
    payload = json.dumps(message, ensure_ascii=False)
    for queue in subscribers:
        queue.put_nowait(payload)


async def refresh_source(client: httpx.AsyncClient, src: dict) -> None:
    jobs = [(g, f, r) for g, campos in RANGES.items() for f, r in campos.items()]
    # Todos os ranges da fonte são consultados em paralelo
    results = await asyncio.gather(
        *(fetch_range(client, src, rng) for _, _, rng in jobs),
        return_exceptions=True,
    )
    for (group, field, _), data in zip(jobs, results):
        if isinstance(data, Exception):  # mantém o último valor em caso de erro
            print(f"[erro] {src['name']} / {field}: {data}")
            continue
        key = f"{src['name']}|{group}|{field}"
        if cache.get(key, {}).get("data") != data:  # só avisa se mudou
            message = {
                "source": src["name"],
                "url": f"https://docs.google.com/spreadsheets/d/{src['sheet_id']}/edit#gid={src['gid']}",
                "group": group,
                "field": field,
                "data": data,
            }
            cache[key] = message
            broadcast(message)


async def poll_loop() -> None:
    async with httpx.AsyncClient(timeout=15) as client:
        while True:
            for src in SOURCES:
                await refresh_source(client, src)
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


@app.get("/opentab.png")
async def opentab_icon():
    return FileResponse("opentab.png")  # deve estar na mesma pasta do app.py


@app.get("/api/data")
async def snapshot():
    """Estado atual completo (útil para testes ou clientes sem SSE)."""
    return list(cache.values())


@app.get("/events")
async def events():
    queue: asyncio.Queue = asyncio.Queue()

    async def stream():
        subscribers.add(queue)
        try:
            # Ao conectar, envia tudo o que já está em cache
            for message in list(cache.values()):
                yield f"data: {json.dumps(message, ensure_ascii=False)}\n\n"
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