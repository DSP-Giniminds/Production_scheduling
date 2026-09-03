"""
Production scheduling dashboard -- FastAPI + ClickHouse.

Serves both a browser-viewable dashboard page (/) and a JSON API (/api/...)
that other tools could call independently of the page -- e.g. a future real
ERP system could poll /api/reschedules/recent instead of only consuming the
Kafka topic directly.

Run with:
    uvicorn main:app --host 0.0.0.0 --port 8000 --reload
"""

import os

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

import db

app = FastAPI(title="Production Scheduling Dashboard")

_TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")
templates = Jinja2Templates(directory=_TEMPLATE_DIR)


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/machines/summary")
def machines_summary():
    return db.machine_status_summary()


@app.get("/api/machines/list")
def machines_list():
    return db.machine_status_list()


@app.get("/api/orders/summary")
def orders_summary():
    return db.order_status_summary()


@app.get("/api/reschedules/recent")
def reschedules_recent(limit: int = 25):
    return db.recent_reschedules(limit)


@app.get("/api/reschedules/trend")
def reschedules_trend():
    return db.tardiness_trend()
