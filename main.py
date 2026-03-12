import sys
from pathlib import Path

# Strong path fix for Render Docker (this solves the ModuleNotFoundError)
root_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(root_dir))
sys.path.insert(0, "/app")
sys.path.insert(0, str(root_dir / "src"))

print("DEBUG: Python path =", sys.path)  # temporary debug - we can remove later

from fastapi import FastAPI
from src.core.extraction_engine import app as engine_app

app = FastAPI(title="TabularExtract")

# Mount the full modular engine
app.mount("/", engine_app)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
