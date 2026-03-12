import sys
from pathlib import Path

# Fix for Render (and any deployment) to find the src folder
root_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(root_dir))

from fastapi import FastAPI
from src.core.extraction_engine import app as engine_app

app = FastAPI(title="TabularExtract")

# Mount the full engine (all logic, UI, Stripe, quotas, etc. live in extraction_engine.py)
app.mount("/", engine_app)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
