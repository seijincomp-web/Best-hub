import asyncio
import html
import os
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Optional

import discord
from cryptography.fernet import Fernet, InvalidToken
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Form, HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING
from bson import ObjectId

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "").strip()
DATABASE_NAME = os.getenv("DATABASE_NAME", "discord_host_test").strip()
ADMIN_USER = os.getenv("ADMIN_USER", "admin").strip()
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin").strip()
FERNET_KEY = os.getenv("FERNET_KEY", "").strip()
AUTO_RESTART_ON_BOOT = os.getenv("AUTO_RESTART_ON_BOOT", "false").lower() == "true"

if not MONGO_URI:
    raise RuntimeError("MONGO_URI não configurada.")

if not FERNET_KEY:
    raise RuntimeError("FERNET_KEY não configurada. Gere com: python generate_key.py")

fernet = Fernet(FERNET_KEY.encode())
security = HTTPBasic()
mongo_client: Optional[AsyncIOMotorClient] = None
bots_col = None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def enc_token(token: str) -> str:
    return fernet.encrypt(token.encode()).decode()


def dec_token(token_encrypted: str) -> str:
    return fernet.decrypt(token_encrypted.encode()).decode()


def esc(value: Any) -> str:
    return html.escape(str(value or ""))


def oid(bot_id: str) -> ObjectId:
    try:
        return ObjectId(bot_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Bot não encontrado.")


def require_auth(credentials: HTTPBasicCredentials = Depends(security)):
    user_ok = secrets.compare_digest(credentials.username, ADMIN_USER)
    pass_ok = secrets.compare_digest(credentials.password, ADMIN_PASSWORD)
    if not (user_ok and pass_ok):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Login inválido.",
            headers={"WWW-Authenticate": "Basic"},
        )
    return True


class BotRuntime:
    def __init__(self, bot_id: str, token: str):
        self.bot_id = bot_id
        self.token = token
        self.client: Optional[discord.Client] = None
        self.task: Optional[asyncio.Task] = None
        self.started_at: Optional[str] = None
        self.user_label: Optional[str] = None
        self.last_error: Optional[str] = None

    async def start(self):
        if self.task and not self.task.done():
            return

        intents = discord.Intents.default()
        # message_content fica False para não exigir intent privilegiada no Developer Portal.
        client = discord.Client(intents=intents)
        self.client = client
        self.started_at = now_iso()
        self.last_error = None

        @client.event
        async def on_ready():
            self.user_label = f"{client.user} ({client.user.id})" if client.user else "Online"
            await bots_col.update_one(
                {"_id": oid(self.bot_id)},
                {
                    "$set": {
                        "status": "online",
                        "discord_user": self.user_label,
                        "last_error": None,
                        "started_at": self.started_at,
                        "updated_at": now_iso(),
                    }
                },
            )

        async def runner():
            try:
                await bots_col.update_one(
                    {"_id": oid(self.bot_id)},
                    {"$set": {"status": "starting", "last_error": None, "updated_at": now_iso()}},
                )
                await client.start(self.token, reconnect=True)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.last_error = f"{type(e).__name__}: {e}"
                await bots_col.update_one(
                    {"_id": oid(self.bot_id)},
                    {
                        "$set": {
                            "status": "error",
                            "last_error": self.last_error,
                            "updated_at": now_iso(),
                        }
                    },
                )
            finally:
                if not client.is_closed():
                    try:
                        await client.close()
                    except Exception:
                        pass
                # Se o usuário clicou OFF, o status já vira off. Se caiu sozinho, marca offline.
                doc = await bots_col.find_one({"_id": oid(self.bot_id)})
                if doc and doc.get("status") in {"online", "starting"}:
                    await bots_col.update_one(
                        {"_id": oid(self.bot_id)},
                        {"$set": {"status": "off", "updated_at": now_iso()}},
                    )

        self.task = asyncio.create_task(runner())

    async def stop(self):
        if self.client and not self.client.is_closed():
            await self.client.close()
        if self.task and not self.task.done():
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        await bots_col.update_one(
            {"_id": oid(self.bot_id)},
            {"$set": {"status": "off", "updated_at": now_iso()}},
        )


running_bots: dict[str, BotRuntime] = {}


async def get_bot_doc(bot_id: str) -> dict:
    doc = await bots_col.find_one({"_id": oid(bot_id)})
    if not doc:
        raise HTTPException(status_code=404, detail="Bot não encontrado.")
    return doc


async def start_saved_bot(bot_id: str):
    doc = await get_bot_doc(bot_id)
    try:
        token = dec_token(doc["token_encrypted"])
    except InvalidToken:
        await bots_col.update_one(
            {"_id": oid(bot_id)},
            {"$set": {"status": "error", "last_error": "FERNET_KEY não consegue descriptografar este token.", "updated_at": now_iso()}},
        )
        return

    runtime = running_bots.get(bot_id)
    if runtime and runtime.task and not runtime.task.done():
        return

    runtime = BotRuntime(bot_id=bot_id, token=token)
    running_bots[bot_id] = runtime
    await runtime.start()


async def stop_saved_bot(bot_id: str):
    runtime = running_bots.get(bot_id)
    if runtime:
        await runtime.stop()
        running_bots.pop(bot_id, None)
    else:
        await bots_col.update_one(
            {"_id": oid(bot_id)},
            {"$set": {"status": "off", "updated_at": now_iso()}},
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    global mongo_client, bots_col
    mongo_client = AsyncIOMotorClient(MONGO_URI)
    db = mongo_client[DATABASE_NAME]
    bots_col = db["bots"]
    await bots_col.create_index([("created_at", ASCENDING)])
    await bots_col.update_many(
        {"status": {"$in": ["online", "starting"]}},
        {"$set": {"status": "off", "updated_at": now_iso()}},
    )

    if AUTO_RESTART_ON_BOOT:
        cursor = bots_col.find({"auto_restart": True})
        async for doc in cursor:
            await start_saved_bot(str(doc["_id"]))

    try:
        yield
    finally:
        for runtime in list(running_bots.values()):
            try:
                await runtime.stop()
            except Exception:
                pass
        running_bots.clear()
        mongo_client.close()


app = FastAPI(title="Discord Token Host", lifespan=lifespan)


STYLE = """
<style>
:root { color-scheme: dark; }
body { margin: 0; font-family: Arial, sans-serif; background: #09090b; color: #f4f4f5; }
main { max-width: 1050px; margin: 0 auto; padding: 28px 14px 60px; }
.header { display: flex; justify-content: space-between; align-items: center; gap: 12px; margin-bottom: 18px; }
.brand h1 { margin: 0; font-size: 28px; }
.brand p { margin: 6px 0 0; color: #a1a1aa; }
.card { background: #18181b; border: 1px solid #27272a; border-radius: 18px; padding: 18px; margin: 14px 0; box-shadow: 0 14px 45px rgba(0,0,0,.22); }
.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(230px, 1fr)); gap: 12px; }
label { display: block; color: #d4d4d8; font-weight: 700; margin: 0 0 7px; }
input { width: 100%; box-sizing: border-box; background: #09090b; color: #fafafa; border: 1px solid #3f3f46; border-radius: 12px; padding: 12px; outline: none; }
input:focus { border-color: #a1a1aa; }
button, .btn { border: 0; border-radius: 12px; padding: 11px 14px; font-weight: 800; cursor: pointer; text-decoration: none; display: inline-flex; align-items: center; justify-content: center; gap: 8px; color: white; background: #3f3f46; }
.btn-on { background: #15803d; }
.btn-off { background: #b91c1c; }
.btn-del { background: #7f1d1d; }
.btn-small { padding: 8px 10px; font-size: 13px; }
.row { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
.bot-title { display: flex; justify-content: space-between; gap: 12px; align-items: flex-start; }
.status { padding: 6px 10px; border-radius: 999px; font-size: 12px; font-weight: 900; background: #3f3f46; text-transform: uppercase; }
.status.online { background: #166534; }
.status.starting { background: #854d0e; }
.status.error { background: #991b1b; }
.status.off { background: #3f3f46; }
.muted { color: #a1a1aa; }
.err { color: #fca5a5; white-space: pre-wrap; word-break: break-word; }
.warn { border: 1px solid #713f12; background: #1c1917; color: #fde68a; }
code { background: #09090b; padding: 3px 6px; border-radius: 8px; }
hr { border: 0; border-top: 1px solid #27272a; margin: 14px 0; }
</style>
"""


def layout(content: str) -> HTMLResponse:
    return HTMLResponse(
        f"""<!doctype html>
<html lang="pt-br">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Discord Token Host</title>
{STYLE}
</head>
<body><main>{content}</main></body></html>"""
    )


@app.get("/", response_class=HTMLResponse)
async def home(_: bool = Depends(require_auth)):
    bots = []
    cursor = bots_col.find({}).sort("created_at", -1)
    async for doc in cursor:
        bot_id = str(doc["_id"])
        status_value = doc.get("status", "off")
        runtime = running_bots.get(bot_id)
        truly_running = runtime and runtime.task and not runtime.task.done()
        status_label = status_value
        if truly_running and status_value == "off":
            status_label = "online"

        delete_form = ""
        if status_value in {"off", "error"}:
            delete_form = f"""
            <form method="post" action="/bots/{bot_id}/delete" onsubmit="return confirm('Excluir este bot do painel?')">
                <button class="btn-del btn-small">Excluir</button>
            </form>
            """

        action = ""
        if status_value in {"online", "starting"} or truly_running:
            action = f"""
            <form method="post" action="/bots/{bot_id}/stop">
                <button class="btn-off">Desligar</button>
            </form>
            """
        else:
            action = f"""
            <form method="post" action="/bots/{bot_id}/start">
                <button class="btn-on">Ligar</button>
            </form>
            """

        auto_restart_checked = "checked" if doc.get("auto_restart") else ""
        bots.append(
            f"""
            <div class="card">
                <div class="bot-title">
                    <div>
                        <h2 style="margin:0 0 6px">{esc(doc.get('name', 'Sem nome'))}</h2>
                        <div class="muted">ID interno: <code>{bot_id}</code></div>
                        <div class="muted">Discord: {esc(doc.get('discord_user') or 'Ainda não identificado')}</div>
                    </div>
                    <span class="status {esc(status_label)}">{esc(status_label)}</span>
                </div>
                <hr>
                <div class="row">
                    {action}
                    <form method="post" action="/bots/{bot_id}/auto_restart" class="row">
                        <input type="hidden" name="enabled" value="0">
                        <label class="row" style="margin:0;font-weight:600;color:#d4d4d8">
                            <input type="checkbox" name="enabled" value="1" {auto_restart_checked} style="width:auto"> Auto restart
                        </label>
                        <button class="btn-small">Salvar</button>
                    </form>
                    {delete_form}
                </div>
                <p class="muted">Criado: {esc(doc.get('created_at'))}</p>
                {f'<p class="err">Erro: {esc(doc.get("last_error"))}</p>' if doc.get('last_error') else ''}
            </div>
            """
        )

    bots_html = "".join(bots) if bots else "<div class='card'><p class='muted'>Nenhum bot cadastrado ainda.</p></div>"
    content = f"""
    <div class="header">
        <div class="brand">
            <h1>Discord Token Host</h1>
            <p>Painel teste para ligar/desligar bots Discord via token.</p>
        </div>
        <a class="btn" href="/health">Health</a>
    </div>

    <div class="card warn">
        <b>Modo teste:</b> não coloque token principal de cliente ainda. Esta versão mantém bots no mesmo processo do site.
        Para produção, o certo é isolar cada bot em processo/container próprio.
    </div>

    <div class="card">
        <h2 style="margin-top:0">Adicionar bot</h2>
        <form method="post" action="/bots" class="grid">
            <div>
                <label>Nome do bot</label>
                <input name="name" placeholder="Ex: Sally Teste" required>
            </div>
            <div>
                <label>Token Discord</label>
                <input name="token" placeholder="Cole o token do bot" required>
            </div>
            <div style="display:flex;align-items:end">
                <button class="btn-on" style="width:100%">Salvar bot</button>
            </div>
        </form>
    </div>

    {bots_html}
    """
    return layout(content)


@app.get("/health")
async def health(_: bool = Depends(require_auth)):
    return {
        "ok": True,
        "database": DATABASE_NAME,
        "running_bots": len([r for r in running_bots.values() if r.task and not r.task.done()]),
        "time": now_iso(),
    }


@app.post("/bots")
async def create_bot(
    name: str = Form(...),
    token: str = Form(...),
    _: bool = Depends(require_auth),
):
    name = name.strip()[:80]
    token = token.strip()
    if not name or not token:
        raise HTTPException(status_code=400, detail="Nome e token são obrigatórios.")

    await bots_col.insert_one(
        {
            "name": name,
            "token_encrypted": enc_token(token),
            "status": "off",
            "discord_user": None,
            "last_error": None,
            "auto_restart": False,
            "created_at": now_iso(),
            "updated_at": now_iso(),
        }
    )
    return RedirectResponse("/", status_code=303)


@app.post("/bots/{bot_id}/start")
async def start_bot(bot_id: str, _: bool = Depends(require_auth)):
    await start_saved_bot(bot_id)
    return RedirectResponse("/", status_code=303)


@app.post("/bots/{bot_id}/stop")
async def stop_bot(bot_id: str, _: bool = Depends(require_auth)):
    await stop_saved_bot(bot_id)
    return RedirectResponse("/", status_code=303)


@app.post("/bots/{bot_id}/delete")
async def delete_bot(bot_id: str, _: bool = Depends(require_auth)):
    doc = await get_bot_doc(bot_id)
    if doc.get("status") in {"online", "starting"}:
        raise HTTPException(status_code=400, detail="Desligue o bot antes de excluir.")
    await stop_saved_bot(bot_id)
    await bots_col.delete_one({"_id": oid(bot_id)})
    return RedirectResponse("/", status_code=303)


@app.post("/bots/{bot_id}/auto_restart")
async def set_auto_restart(
    bot_id: str,
    enabled: str = Form("0"),
    _: bool = Depends(require_auth),
):
    await bots_col.update_one(
        {"_id": oid(bot_id)},
        {"$set": {"auto_restart": enabled == "1", "updated_at": now_iso()}},
    )
    return RedirectResponse("/", status_code=303)
