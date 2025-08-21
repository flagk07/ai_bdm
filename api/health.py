from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI()

@app.get("/")
async def health() -> JSONResponse:
	return JSONResponse({"ok": True}) 