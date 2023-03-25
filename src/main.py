import uvicorn
from app.app import create_app
import os

app = create_app()

if __name__ == "__main__":
    if not "static" in os.listdir():
        os.mkdir("static")
    uvicorn.run(
        "main:app",
        reload=True,
        use_colors=True,
        host="0.0.0.0",
        port=8080,
    )
