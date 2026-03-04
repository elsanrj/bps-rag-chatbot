from fastapi import FastAPI

# Inisialisasi aplikasi FastAPI
app = FastAPI()

# Membuat endpoint GET /health
@app.get("/health")
def check_health():
    return {"status": "ok"}