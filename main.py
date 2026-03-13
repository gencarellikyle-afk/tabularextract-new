from fastapi import FastAPI, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
import uvicorn
import os
from io import BytesIO
import zipfile
import tempfile
from src.core.extraction_engine import TableExtractionEngine

app = FastAPI(title="TabularExtract")
last_tables = None

# === YOUR EXACT FRONTEND (unchanged) ===
@app.get("/", response_class=HTMLResponse)
async def home():
    return """[PASTE YOUR ENTIRE HTML/JS BLOCK FROM THE SUMMARY HERE — it's identical]"""

@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    global last_tables
    try:
        content = await file.read()
        engine = TableExtractionEngine()
        result = engine.extract_tables(content)
        last_tables = result.get("tables", [])
        return result
    except Exception as e:
        return JSONResponse({"success": False, "error": str(e)})

@app.get("/download-all")
async def download_all():
    global last_tables
    if not last_tables:
        return JSONResponse({"message": "No tables yet."})
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as z:
        for t in last_tables:
            z.writestr(f"Table_{t['table_id']}_Page_{t['page_numbers'][0]}.csv", t["csv"])
    zip_buffer.seek(0)
    return StreamingResponse(zip_buffer, media_type="application/zip", headers={"Content-Disposition": "attachment; filename=all_tables.zip"})

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
