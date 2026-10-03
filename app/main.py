"""Application factory, lifecycle management, and AWS Lambda handlers."""

import asyncio
import logging
from contextlib import asynccontextmanager
from uuid import uuid4

import boto3
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from mangum import Mangum
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING
from pymongo.errors import PyMongoError

from app.api.orders import router as orders_router
from app.config import settings
from app.constants import AUDIT_COLLECTION, ORDER_COLLECTION, OUTBOX_COLLECTION
from app.dependencies import get_database, get_request_id
from app.services.consumer import run_sqs_lambda_batch, sqs_consumer_loop
from app.services.events import retry_unpublished_events
from app.services.scheduler import advance_pending_orders

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("orders")


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not settings.mongodb_uri:
        raise RuntimeError("MONGODB_URI must be set to a real MongoDB Atlas connection string")
    client = AsyncIOMotorClient(settings.mongodb_uri, serverSelectionTimeoutMS=10_000)
    try:
        await client.admin.command("ping")
        app.state.mongo_client = client
        app.state.database = client[settings.mongodb_database]
        aws_session = boto3.session.Session(region_name=settings.aws_region)
        app.state.eventbridge_client = aws_session.client("events")
        app.state.sqs_client = aws_session.client("sqs")
        app.state.s3_client = aws_session.client("s3")
        await asyncio.to_thread(app.state.eventbridge_client.describe_event_bus, Name=settings.eventbridge_event_bus_name)
        queue_response = await asyncio.to_thread(app.state.sqs_client.get_queue_url, QueueName=settings.sqs_queue_name)
        app.state.sqs_queue_url = queue_response["QueueUrl"]
        await asyncio.to_thread(app.state.s3_client.head_bucket, Bucket=settings.s3_invoice_bucket)
        await app.state.database[ORDER_COLLECTION].create_index([("status", ASCENDING)])
        await app.state.database[ORDER_COLLECTION].create_index([("customer_id", ASCENDING), ("created_at", ASCENDING)])
        await app.state.database[ORDER_COLLECTION].create_index([("idempotency_key_hash", ASCENDING)], unique=True, partialFilterExpression={"idempotency_key_hash": {"$exists": True}})
        await app.state.database[OUTBOX_COLLECTION].create_index([("event_id", ASCENDING)], unique=True)
        await app.state.database[OUTBOX_COLLECTION].create_index([("published_at", ASCENDING), ("created_at", ASCENDING)])
        await app.state.database[AUDIT_COLLECTION].create_index([("event_id", ASCENDING)], unique=True)
        await app.state.database[AUDIT_COLLECTION].create_index([("order_id", ASCENDING), ("processed_at", ASCENDING)])
        if not settings.running_in_lambda:
            scheduler = AsyncIOScheduler(timezone="UTC")
            scheduler.add_job(advance_pending_orders, "interval", minutes=5, args=[app], id="process-pending-orders")
            scheduler.add_job(retry_unpublished_events, "interval", minutes=1, args=[app], id="retry-order-events")
            scheduler.start()
            app.state.scheduler = scheduler
            app.state.sqs_consumer_task = asyncio.create_task(sqs_consumer_loop(app), name="sqs-order-event-consumer")
            logger.info("Local runtime initialized")
        else:
            logger.info("Lambda runtime initialized")
        yield
    except PyMongoError as exc:
        raise RuntimeError("Unable to connect to MongoDB Atlas") from exc
    finally:
        task = getattr(app.state, "sqs_consumer_task", None)
        if task:
            task.cancel()
        scheduler = getattr(app.state, "scheduler", None)
        if scheduler:
            scheduler.shutdown(wait=False)
        client.close()


app = FastAPI(title="Order Processing System", version="0.1.0", description="Real MongoDB Atlas-backed order APIs.", lifespan=lifespan)
app.include_router(orders_router)


@app.middleware("http")
async def add_correlation_id(request: Request, call_next):
    supplied_id = request.headers.get("X-Request-ID", "").strip()
    request.state.request_id = supplied_id[:128] if supplied_id else str(uuid4())
    response = await call_next(request)
    response.headers["X-Request-ID"] = request.state.request_id
    logger.info("%s %s completed with status %s [request_id=%s]", request.method, request.url.path, response.status_code, request.state.request_id)
    return response


@app.exception_handler(HTTPException)
async def standard_http_error(request: Request, exc: HTTPException) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content={"error": {"code": f"HTTP_{exc.status_code}", "message": str(exc.detail), "request_id": get_request_id(request)}})


@app.exception_handler(RequestValidationError)
async def standard_validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
    return JSONResponse(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, content={"error": {"code": "VALIDATION_ERROR", "message": "Request validation failed", "request_id": get_request_id(request)}})


@app.exception_handler(Exception)
async def standard_unexpected_error(request: Request, exc: Exception) -> JSONResponse:
    request_id = getattr(request.state, "request_id", str(uuid4()))
    logger.exception("Unhandled request failure [request_id=%s]", request_id)
    return JSONResponse(status_code=500, content={"error": {"code": "INTERNAL_ERROR", "message": "An unexpected error occurred", "request_id": request_id}})


@app.get("/health", tags=["System"])
async def health_check(request: Request) -> dict[str, str]:
    await get_database(request).command("ping")
    return {"status": "healthy"}


@app.get("/health/ready", tags=["System"])
async def readiness_check(request: Request) -> dict[str, object]:
    try:
        await get_database(request).command("ping")
        await asyncio.gather(asyncio.to_thread(request.app.state.eventbridge_client.describe_event_bus, Name=settings.eventbridge_event_bus_name), asyncio.to_thread(request.app.state.sqs_client.get_queue_attributes, QueueUrl=request.app.state.sqs_queue_url, AttributeNames=["QueueArn"]), asyncio.to_thread(request.app.state.s3_client.head_bucket, Bucket=settings.s3_invoice_bucket))
    except (BotoCoreError, ClientError, PyMongoError) as exc:
        raise HTTPException(status_code=503, detail="A required dependency is unavailable") from exc
    return {"status": "ready", "components": {"mongodb": "healthy", "eventbridge": "healthy", "sqs": "healthy", "s3": "healthy"}}


api_lambda_handler = Mangum(app, lifespan="auto")


async def _run_sqs_handler(event: dict) -> dict:
    async with lifespan(app):
        return await run_sqs_lambda_batch(app, event)


def sqs_lambda_handler(event: dict, context) -> dict:
    return asyncio.run(_run_sqs_handler(event))


async def _run_scheduler_handler() -> dict:
    async with lifespan(app):
        await advance_pending_orders(app)
        await retry_unpublished_events(app)
    return {"status": "completed"}


def scheduler_lambda_handler(event: dict, context) -> dict:
    return asyncio.run(_run_scheduler_handler())
