import os
from pymongo import MongoClient
from dotenv import load_dotenv

# Memuat variabel dari file .env
load_dotenv()

# Ambil URI dari .env
uri = os.getenv("MONGODB_URI")

try:
    # 1. Inisialisasi Client
    client = MongoClient(uri)
    
    # 2. Pilih Database dan Collection (otomatis dibuat jika belum ada)
    db = client["bps-rag-chatbot"]
    collection = db["test_connection"]
    
    # 3. Buat Dokumen Percobaan
    test_document = {
        "nama_proyek": "Lupakan dia brok",
        "fase": "Fase 0: Eksplorasi",
        "status": "Pengin mixue strawberry",
        "hari": 5
    }
    
    # 4. Insert Dokumen
    result = collection.insert_one(test_document)
    
    print("--- Verifikasi Database ---")
    print(f"Koneksi Berhasil! ID Dokumen: {result.inserted_id}")
    
except Exception as e:
    print(f"Koneksi Gagal: {e}")