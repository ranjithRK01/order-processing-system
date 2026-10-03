"""Compatibility entry point for Uvicorn and AWS Lambda."""

from app.main import api_lambda_handler, app, scheduler_lambda_handler, sqs_lambda_handler

__all__ = ["app", "api_lambda_handler", "sqs_lambda_handler", "scheduler_lambda_handler"]
