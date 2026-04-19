import csv
import json
import logging
import os
import random
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

import httpx
from fastapi import Depends, FastAPI, Header, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from sqlalchemy import DateTime, Float, Integer, String, create_engine, func
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

SERVICE_NAME = os.getenv("SERVICE_NAME", "order-service")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./order.db")
SEED_DATA_DIR = Path(os.getenv("SEED_DATA_DIR", ""))
CUSTOMER_SERVICE_URL = os.getenv("CUSTOMER_SERVICE_URL", "http://localhost:8001")
RESTAURANT_SERVICE_URL = os.getenv("RESTAURANT_SERVICE_URL", "http://localhost:8002")
PAYMENT_SERVICE_URL = os.getenv("PAYMENT_SERVICE_URL", "http://localhost:8004")
DELIVERY_SERVICE_URL = os.getenv("DELIVERY_SERVICE_URL", "http://localhost:8005")
NOTIFICATION_SERVICE_URL = os.getenv("NOTIFICATION_SERVICE_URL", "http://localhost:8006")
DELIVERY_FEE = float(os.getenv("DELIVERY_FEE", "40"))

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(SERVICE_NAME)
REQUEST_COUNT = Counter("http_requests_total", "Total HTTP requests", ["service", "path", "method", "status_code"])
REQUEST_LATENCY = Histogram("http_request_duration_seconds", "Request latency", ["service", "path", "method"])
ORDERS_PLACED = Counter("orders_placed_total", "Orders placed", ["service", "status"])


class Base(DeclarativeBase):
    pass


class Order(Base):
    __tablename__ = "orders"
    order_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    customer_id: Mapped[int] = mapped_column(Integer)
    restaurant_id: Mapped[int] = mapped_column(Integer)
    restaurant_name: Mapped[str] = mapped_column(String(150))
    address_id: Mapped[int] = mapped_column(Integer)
    address_city: Mapped[str] = mapped_column(String(120))
    order_status: Mapped[str] = mapped_column(String(30))
    subtotal: Mapped[float] = mapped_column(Float)
    tax_amount: Mapped[float] = mapped_column(Float)
    delivery_fee: Mapped[float] = mapped_column(Float)
    order_total: Mapped[float] = mapped_column(Float)
    payment_status: Mapped[str] = mapped_column(String(30))
    created_at: Mapped[datetime] = mapped_column(DateTime)


class OrderItem(Base):
    __tablename__ = "order_items"
    order_item_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    order_id: Mapped[int] = mapped_column(Integer)
    item_id: Mapped[int] = mapped_column(Integer)
    item_name: Mapped[str] = mapped_column(String(150))
    quantity: Mapped[int] = mapped_column(Integer)
    price: Mapped[float] = mapped_column(Float)


class OrderRequest(Base):
    __tablename__ = "order_requests"
    request_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    idempotency_key: Mapped[str] = mapped_column(String(120), unique=True)
    order_id: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime)


engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


class ApiError(Exception):
    def __init__(self, code: str, message: str, status_code: int = 400):
        self.code = code
        self.message = message
        self.status_code = status_code


class RequestedItem(BaseModel):
    item_id: int
    quantity: int


class PaymentPayload(BaseModel):
    method: str
    reference: Optional[str] = None
    simulate_failure: bool = False


class OrderCreate(BaseModel):
    customer_id: int
    restaurant_id: int
    address_id: int
    items: list[RequestedItem]
    payment: PaymentPayload


class OrderItemOut(BaseModel):
    order_item_id: int
    order_id: int
    item_id: int
    item_name: str
    quantity: int
    price: float

    class Config:
        from_attributes = True


class OrderOut(BaseModel):
    order_id: int
    customer_id: int
    restaurant_id: int
    restaurant_name: str
    address_id: int
    address_city: str
    order_status: str
    subtotal: float
    tax_amount: float
    delivery_fee: float
    order_total: float
    payment_status: str
    created_at: datetime

    class Config:
        from_attributes = True


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def next_id(db: Session, model, column) -> int:
    return (db.query(func.max(column)).scalar() or 0) + 1


def parse_dt(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def seed_data():
    if not SEED_DATA_DIR or not SEED_DATA_DIR.exists():
        return
    with SessionLocal() as db:
        if db.query(Order).first():
            return
        with (SEED_DATA_DIR / "ofd_orders.csv").open(newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                db.add(Order(order_id=int(row["order_id"]), customer_id=int(row["customer_id"]), restaurant_id=int(row["restaurant_id"]), restaurant_name=f"Restaurant {row['restaurant_id']}", address_id=int(row["address_id"]), address_city="UNKNOWN", order_status=row["order_status"], subtotal=float(row["order_total"]), tax_amount=0.0, delivery_fee=0.0, order_total=float(row["order_total"]), payment_status=row["payment_status"], created_at=parse_dt(row["created_at"])))
        with (SEED_DATA_DIR / "ofd_order_items.csv").open(newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                db.add(OrderItem(order_item_id=int(row["order_item_id"]), order_id=int(row["order_id"]), item_id=int(row["item_id"]), item_name=f"Item {row['item_id']}", quantity=int(row["quantity"]), price=float(row["price"])))
        db.commit()


async def call_service(method: str, url: str, correlation_id: str, payload: Optional[dict] = None, extra_headers: Optional[dict] = None):
    headers = {"X-Correlation-Id": correlation_id}
    if extra_headers:
        headers.update(extra_headers)
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                response = await client.request(method, url, json=payload, headers=headers)
            if response.status_code >= 500 and attempt < 2:
                time.sleep(0.05 + random.random() * 0.1)
                continue
            if response.status_code >= 400:
                data = response.json()
                raise ApiError(data.get("code", "UPSTREAM_ERROR"), data.get("message", f"Upstream call failed: {url}"), response.status_code)
            return response.json()
        except httpx.RequestError:
            if attempt == 2:
                raise ApiError("UPSTREAM_UNAVAILABLE", f"Unable to reach {url}", 503)
            time.sleep(0.05 + random.random() * 0.1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    seed_data()
    yield


app = FastAPI(title="Order Service", version="1.0.0", lifespan=lifespan)


@app.middleware("http")
async def telemetry(request: Request, call_next):
    correlation_id = request.headers.get("X-Correlation-Id", str(uuid.uuid4()))
    request.state.correlation_id = correlation_id
    started = time.perf_counter()
    response = await call_next(request)
    elapsed = time.perf_counter() - started
    REQUEST_COUNT.labels(SERVICE_NAME, request.url.path, request.method, str(response.status_code)).inc()
    REQUEST_LATENCY.labels(SERVICE_NAME, request.url.path, request.method).observe(elapsed)
    response.headers["X-Correlation-Id"] = correlation_id
    logger.info(json.dumps({"service": SERVICE_NAME, "correlationId": correlation_id, "path": request.url.path, "statusCode": response.status_code, "latencyMs": round(elapsed * 1000, 2)}))
    return response


@app.exception_handler(ApiError)
async def api_error_handler(request: Request, exc: ApiError):
    return JSONResponse(status_code=exc.status_code, content={"code": exc.code, "message": exc.message, "correlationId": getattr(request.state, "correlation_id", str(uuid.uuid4()))})


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME}


@app.get("/metrics")
def metrics():
    return PlainTextResponse(generate_latest().decode("utf-8"), media_type=CONTENT_TYPE_LATEST)


@app.get("/v1/orders", response_model=list[OrderOut])
def list_orders(customer_id: Optional[int] = None, status: Optional[str] = None, limit: int = Query(10, ge=1, le=100), offset: int = Query(0, ge=0), db: Session = Depends(get_db)):
    query = db.query(Order)
    if customer_id:
        query = query.filter(Order.customer_id == customer_id)
    if status:
        query = query.filter(Order.order_status == status)
    return query.order_by(Order.order_id.desc()).offset(offset).limit(limit).all()


@app.get("/v1/orders/{order_id}")
def get_order(order_id: int, db: Session = Depends(get_db)):
    order = db.get(Order, order_id)
    if not order:
        raise ApiError("ORDER_NOT_FOUND", f"Order {order_id} not found", 404)
    items = db.query(OrderItem).filter(OrderItem.order_id == order_id).all()
    return {**OrderOut.model_validate(order).model_dump(), "items": [OrderItemOut.model_validate(item).model_dump() for item in items]}


@app.post("/v1/orders", status_code=201)
async def create_order(payload: OrderCreate, request: Request, db: Session = Depends(get_db), idempotency_key: Optional[str] = Header(default=None, alias="Idempotency-Key")):
    if not idempotency_key:
        raise ApiError("MISSING_IDEMPOTENCY_KEY", "Idempotency-Key header is required", 400)
    existing_request = db.query(OrderRequest).filter(OrderRequest.idempotency_key == idempotency_key).first()
    if existing_request:
        return get_order(existing_request.order_id, db)
    if not payload.items:
        raise ApiError("INVALID_ORDER", "At least one item is required", 400)
    if len(payload.items) > 20:
        raise ApiError("ITEM_LIMIT_EXCEEDED", "Orders can contain at most 20 items", 400)
    if any(item.quantity > 5 or item.quantity < 1 for item in payload.items):
        raise ApiError("INVALID_ITEM_QUANTITY", "Each item quantity must be between 1 and 5", 400)

    correlation_id = request.state.correlation_id
    address = await call_service("GET", f"{CUSTOMER_SERVICE_URL}/v1/internal/addresses/{payload.address_id}", correlation_id)
    if address["customer_id"] != payload.customer_id:
        raise ApiError("ADDRESS_CUSTOMER_MISMATCH", "Address does not belong to the customer", 409)
    availability = await call_service("POST", f"{RESTAURANT_SERVICE_URL}/v1/internal/availability-check", correlation_id, {"restaurant_id": payload.restaurant_id, "items": [item.model_dump() for item in payload.items]})
    if address["city"].strip().lower() != availability["restaurant"]["city"].strip().lower():
        raise ApiError("CITY_MISMATCH", "Delivery address city must match restaurant city", 409)

    subtotal = round(sum(item["line_total"] for item in availability["items"]), 2)
    tax_amount = round(subtotal * 0.05, 2)
    order_total = round(subtotal + tax_amount + DELIVERY_FEE, 2)

    order = Order(order_id=next_id(db, Order, Order.order_id), customer_id=payload.customer_id, restaurant_id=payload.restaurant_id, restaurant_name=availability["restaurant"]["name"], address_id=payload.address_id, address_city=address["city"], order_status="CREATED", subtotal=subtotal, tax_amount=tax_amount, delivery_fee=DELIVERY_FEE, order_total=order_total, payment_status="PENDING", created_at=datetime.utcnow())
    db.add(order)
    db.flush()

    payment = await call_service("POST", f"{PAYMENT_SERVICE_URL}/v1/payments/charge", correlation_id, {"order_id": order.order_id, "amount": order_total, "method": payload.payment.method, "reference": payload.payment.reference, "simulate_failure": payload.payment.simulate_failure}, {"Idempotency-Key": idempotency_key})
    order.order_status = "CONFIRMED" if payment["status"] in {"SUCCESS", "PENDING"} else "PAYMENT_FAILED"
    order.payment_status = payment["status"]

    next_order_item_id = next_id(db, OrderItem, OrderItem.order_item_id)
    for offset, snapshot in enumerate(availability["items"]):
        db.add(OrderItem(order_item_id=next_order_item_id + offset, order_id=order.order_id, item_id=snapshot["item_id"], item_name=snapshot["name"], quantity=snapshot["quantity"], price=snapshot["price"]))
    db.add(OrderRequest(request_id=next_id(db, OrderRequest, OrderRequest.request_id), idempotency_key=idempotency_key, order_id=order.order_id, created_at=datetime.utcnow()))
    db.commit()
    db.refresh(order)

    delivery = None
    notification = None
    if order.order_status == "CONFIRMED":
        delivery = await call_service("POST", f"{DELIVERY_SERVICE_URL}/v1/deliveries/assign", correlation_id, {"order_id": order.order_id, "city": address["city"]})
        notification = await call_service("POST", f"{NOTIFICATION_SERVICE_URL}/v1/notifications/send", correlation_id, {"order_id": order.order_id, "customer_id": payload.customer_id, "event_type": "ORDER_CONFIRMED", "channel": "EMAIL", "message": f"Order {order.order_id} confirmed with payment status {order.payment_status}"})

    ORDERS_PLACED.labels(SERVICE_NAME, order.order_status).inc()
    return {**OrderOut.model_validate(order).model_dump(), "items": availability["items"], "payment": payment, "delivery": delivery, "notification": notification}


@app.post("/v1/orders/{order_id}/cancel")
def cancel_order(order_id: int, db: Session = Depends(get_db)):
    order = db.get(Order, order_id)
    if not order:
        raise ApiError("ORDER_NOT_FOUND", f"Order {order_id} not found", 404)
    if order.order_status == "DELIVERED":
        raise ApiError("ORDER_CANCEL_NOT_ALLOWED", "Delivered orders cannot be cancelled", 409)
    order.order_status = "CANCELLED"
    db.commit()
    return {"order_id": order.order_id, "order_status": order.order_status}
