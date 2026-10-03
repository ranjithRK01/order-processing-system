"""Invoice PDF generation and private S3 storage."""

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from io import BytesIO

from fastapi import FastAPI
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from app.config import settings
from app.constants import ORDER_COLLECTION
from app.dependencies import parse_order_id


def generate_invoice_pdf(order: dict) -> bytes:
    output = BytesIO()
    document = SimpleDocTemplate(output, pagesize=A4, rightMargin=18 * mm, leftMargin=18 * mm, topMargin=18 * mm, bottomMargin=18 * mm, title=f"Invoice {order['_id']}")
    styles = getSampleStyleSheet()
    story = [Paragraph("Order Invoice", styles["Title"]), Spacer(1, 8 * mm), Paragraph(f"Invoice reference: {order['_id']}", styles["BodyText"]), Paragraph(f"Customer: {order['customer_id']}", styles["BodyText"]), Paragraph(f"Order date: {order['created_at'].astimezone(UTC).strftime('%Y-%m-%d %H:%M UTC')}", styles["BodyText"]), Spacer(1, 6 * mm)]
    rows = [["Product", "Quantity", "Unit price", "Line total"]]
    for item in order["items"]:
        rows.append([Paragraph(str(item["name"]), styles["BodyText"]), str(item["quantity"]), f"{Decimal(str(item['unit_price'])):.2f}", f"{Decimal(str(item['line_total'])):.2f}"])
    rows.append(["", "", "Total", f"{Decimal(str(order['total_amount'])):.2f}"])
    table = Table(rows, colWidths=[72 * mm, 24 * mm, 36 * mm, 36 * mm], repeatRows=1)
    table.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f4e78")), ("TEXTCOLOR", (0, 0), (-1, 0), colors.white), ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"), ("ALIGN", (1, 1), (-1, -1), "RIGHT"), ("FONTNAME", (2, -1), (-1, -1), "Helvetica-Bold"), ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#b7c9d6")), ("VALIGN", (0, 0), (-1, -1), "MIDDLE"), ("TOPPADDING", (0, 0), (-1, -1), 8), ("BOTTOMPADDING", (0, 0), (-1, -1), 8)]))
    story.extend([table, Spacer(1, 8 * mm), Paragraph("Thank you for your order.", styles["BodyText"])])
    document.build(story)
    return output.getvalue()


async def ensure_invoice_uploaded(app: FastAPI, order_id: str, event_id: str) -> None:
    order = await app.state.database[ORDER_COLLECTION].find_one({"_id": parse_order_id(order_id)})
    if not order:
        raise ValueError(f"Order {order_id} does not exist for invoice generation")
    if order.get("invoice", {}).get("s3_key"):
        return
    s3_key = f"invoices/{order_id}/invoice.pdf"
    invoice_bytes = await asyncio.to_thread(generate_invoice_pdf, order)
    await asyncio.to_thread(app.state.s3_client.put_object, Bucket=settings.s3_invoice_bucket, Key=s3_key, Body=invoice_bytes, ContentType="application/pdf", ServerSideEncryption="AES256", Metadata={"order-id": order_id, "event-id": event_id})
    await app.state.database[ORDER_COLLECTION].update_one({"_id": order["_id"], "invoice.s3_key": {"$exists": False}}, {"$set": {"invoice": {"s3_bucket": settings.s3_invoice_bucket, "s3_key": s3_key, "content_type": "application/pdf", "generated_at": datetime.now(UTC)}}})
