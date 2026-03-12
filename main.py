import sys
from pathlib import Path

# Strongest path fix for Render Docker
base_dir = Path(__file__).resolve().parent
sys.path = [
    str(base_dir),
    str(base_dir / "src"),
    "/app",
    "/app/src",
    str(Path.cwd()),
    ""
] + sys.path

print("DEBUG PYTHON PATH:", sys.path)

from fastapi import FastAPI
from src.core.extraction_engine import app as engine_app

app = FastAPI(title="TabularExtract")

# Mount the modular engine
app.mount("/", engine_app)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
