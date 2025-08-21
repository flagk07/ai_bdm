from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI()

@app.get("/api/health")
async def health() -> JSONResponse:
	return JSONResponse({"ok": True}) 