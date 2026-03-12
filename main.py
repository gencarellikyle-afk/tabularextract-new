import sys
import os
sys.path.insert(0, os.path.abspath("."))
from fastapi import FastAPI
from src.core.extraction_engine import app as engine_app

app = FastAPI(title="TabularExtract")

# Mount the modular engine
app.mount("/", engine_app)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
