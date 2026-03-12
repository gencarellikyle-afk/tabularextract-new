import sys
from pathlib import Path
import os

print("DEBUG: Current dir =", os.getcwd())
print("DEBUG: Files in /app =", os.listdir("/app") if os.path.exists("/app") else "no /app")
if os.path.exists("/app/src"):
    print("DEBUG: Files in /app/src =", os.listdir("/app/src"))
if os.path.exists("/app/src/core"):
    print("DEBUG: Files in /app/src/core =", os.listdir("/app/src/core"))

# Strongest path fix
base_dir = Path(__file__).resolve().parent
sys.path = [
    str(base_dir),
    str(base_dir / "src"),
    "/app",
    "/app/src",
    str(base_dir / "src" / "core"),
    ""
] + sys.path

print("DEBUG: Final PYTHON PATH =", sys.path)

from fastapi import FastAPI
from src.core.extraction_engine import app as engine_app

app = FastAPI(title="TabularExtract")

app.mount("/", engine_app)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
