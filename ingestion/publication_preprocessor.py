"""
publikasi_preprocessor.py
Fase 1A - Hari 7: Preprocessing Data Publikasi → Unified Document Schema

Tanggung jawab modul ini:
  - Baca raw_docs (source_type: publikasi) dari MongoDB
  - Bersihkan field abstract dari HTML entities dan tag
  - Konversi abstract menjadi content teks bersih siap embedding
  - Simpan hasil ke MongoDB collection `documents` (unified schema)

Strategi chunking publikasi:
  1 publikasi = 1 dokumen — abstract sudah merupakan ringkasan padat.
  Tidak dilakukan chunking di tahap ini karena abstract berfungsi
  sebagai satu unit semantik yang utuh.
  (Chunking PDF publikasi ditangani di fase embedding jika diperlukan.)

Prasyarat:
  - publikasi_ingestor.py sudah dijalankan (raw_docs terisi)
  - cleaner.py tersedia di direktori yang sama

Jalankan:
  python -m ingestion.publikasi_preprocessor

Env variables yang diperlukan (.env):
  MONGODB_URI : Connection string MongoDB Atlas
  MONGODB_DB  : Nama database (contoh: bps_rag)
"""

import os
import logging

from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

from ingestion.cleaner import (
    clean_html,
    normalize_whitespace,
    validate_doc,
    generate_id,
)

from ingestion.regions import DISTRICTS, VILLAGES

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

MONGODB_URI  = os.getenv("MONGODB_URI")
MONGODB_DB   = os.getenv("MONGODB_DB")
RAW_COLL     = "raw_docs"
DOC_COLL     = "documents"
BATCH_SIZE   = 100

def resolve_region(title: str) -> str:
    """
    Cari nama kecamatan atau kelurahan dalam title secara case-insensitive.
    Return nama wilayah jika ditemukan, default 'Kota Bandung' jika tidak.
    Prioritas: kecamatan lebih panjang dicek dulu untuk hindari partial match.
    """
    title_low = title.lower()
    # Strategi: Urutkan kunci berdasarkan panjang karakter (descending) 
    # agar nama panjang tidak terpotong (misal: "Bandung Kidul" vs "Bandung")
    sorted_villages = sorted(VILLAGES.items(), key=len, reverse=True)
    sorted_districts = sorted(DISTRICTS.items(), key=len, reverse=True)
    
    for name, display in sorted_villages:
        if name in title_low: return display

    for name, display in sorted_districts:
        if name in title_low: return display
        
    return "Kota Bandung"

# ---------------------------------------------------------------------------
# Konversi abstract ke teks bersih
# ---------------------------------------------------------------------------

def build_content(title: str, abstract: str) -> str:
    """
    Bersihkan dan gabungkan title + abstract menjadi content siap embedding.

    Abstract publikasi berformat teks naratif (kadang mengandung HTML entities).
    Tidak ada konversi struktural seperti dynamic data atau SIMDASI —
    cukup strip HTML dan normalisasi whitespace.

    Template:
      {title}. {abstract_bersih}

    Title disertakan dalam content agar saat embedding, kata kunci
    dari judul turut memperkuat konteks similarity search.

    Return string kosong jika abstract kosong setelah dibersihkan.
    """
    clean_title    = normalize_whitespace(clean_html(title))
    clean_abstract = normalize_whitespace(clean_html(abstract))
    
    # Daftar abstrak tidak valid
    invalid_abstracts = ["", "-", ".", "tidak ada abstrak", "abstrak belum tersedia", "abstrak tidak tersedia"]

    if not clean_abstract or clean_abstract in invalid_abstracts:
        if not clean_title:
            return None
        # Tambahkan kalimat penanda agar LLM paham kenapa informasinya sedikit
        return f"Judul Publikasi: {clean_title}. (Catatan: Abstrak tidak tersedia untuk publikasi ini, silakan rujuk tautan PDF untuk isi lengkapnya)."

    # Gabungkan title + abstract sebagai satu unit content
    content = f"{clean_title}. {clean_abstract}" if clean_title else clean_abstract
    return normalize_whitespace(content)


# ---------------------------------------------------------------------------
# Proses satu raw publikasi
# ---------------------------------------------------------------------------

def process_one_publikasi(raw_doc: dict) -> dict | None:
    """
    Proses satu raw publikasi dari MongoDB menjadi satu unified document.
    Publikasi menghasilkan tepat satu dokumen per record (1 abstract = 1 chunk).

    Parameter:
      raw_doc : Satu dokumen dari collection raw_docs (source_type: publikasi)

    Return unified document dict atau None jika tidak valid.
    """
    pub_id     = raw_doc.get("pub_id")
    source_url = raw_doc.get("source_url")

    # Payload adalah item langsung dari response API list publikasi
    # Verifikasi struktur sebelum akses field
    payload = raw_doc.get("payload", {})

    if not payload:
        logger.warning(f"Payload kosong untuk pub_id={pub_id}")
        return None

    # Ambil field-field penting dari payload
    # Selalu akses dari payload langsung — tidak ada wrapper "data" di sini
    title       = payload.get("title", "")
    abstract    = payload.get("abstract", "")
    rl_date     = payload.get("rl_date", "")
    updt_date   = payload.get("updt_date", "")
    issn        = payload.get("issn")
    subject_csa = payload.get("subject_csa", [])    # array string kategori
    pdf_url     = payload.get("pdf", "")

    content = build_content(title, abstract)

    if not content:
        logger.info(f"Content kosong setelah cleaning untuk pub_id={pub_id}, skip.")
        return None

    candidate = {"content": content, "id": generate_id(content)}
    if not validate_doc(candidate):
        logger.info(f"Validasi gagal untuk pub_id={pub_id}")
        return None

    # Tentukan tanggal — prioritaskan rl_date, fallback ke updt_date
    date = rl_date or updt_date or None

    # subject_csa adalah array — simpan sebagai list, ambil item pertama
    # sebagai category utama untuk filtering
    category = subject_csa[0] if isinstance(subject_csa, list) and subject_csa else ""

    return {
        "id"          : candidate["id"],
        "source_type" : "publication",
        "title"       : normalize_whitespace(clean_html(title)),
        "content"     : content,
        "metadata"    : {
            "date"       : date,
            "year"       : int(date[:4]) if date and date[:4].isdigit() else None,
            "category"   : category,
            "region"     : resolve_region(title),
            "source_url" : source_url,
            "extra"      : {
                "pub_id"      : pub_id,
                "subject_csa" : subject_csa,  # simpan semua kategori di extra
                "issn"        : issn,
                "pdf_url"     : pdf_url,      # untuk supplementary PDF di fase berikutnya
            },
        },
        "processed_at" : datetime.utcnow(),
    }


# ---------------------------------------------------------------------------
# Simpan ke collection documents
# ---------------------------------------------------------------------------

def save_documents(docs: list[dict], collection) -> int:
    """
    Upsert list unified documents ke MongoDB collection `documents`.
    Upsert berdasarkan field `id` (MD5 hash dari content) — idempotent.
    Return jumlah dokumen yang berhasil disimpan.
    """
    if not docs:
        return 0

    operations = [
        UpdateOne(
            filter={"id": doc["id"]},
            update={"$set": doc},
            upsert=True,
        )
        for doc in docs
    ]

    try:
        result = collection.bulk_write(operations, ordered=False)
        return result.upserted_count + result.modified_count
    except BulkWriteError as e:
        logger.error(f"BulkWriteError: {e.details}")
        return 0


# ---------------------------------------------------------------------------
# Orchestrator utama
# ---------------------------------------------------------------------------

def run():
    """
    Pipeline utama preprocessing publikasi:
      1. Iterasi semua raw_docs (source_type: publikasi)
      2. Bersihkan abstract → unified document
      3. Upsert ke collection `documents`
    """
    if not MONGODB_URI:
        logger.error("MONGODB_URI tidak ditemukan di environment. Hentikan.")
        return

    client   = MongoClient(MONGODB_URI)
    db       = client[MONGODB_DB]
    raw_coll = db[RAW_COLL]
    doc_coll = db[DOC_COLL]

    logger.info(f"Terhubung ke MongoDB: {MONGODB_DB}")

    total_raw   = raw_coll.count_documents({"source_type": "publikasi"})
    total_saved = 0
    total_skip  = 0
    batch       = []

    logger.info(f"Total raw publikasi yang akan diproses: {total_raw}")

    for i, raw_doc in enumerate(raw_coll.find({"source_type": "publikasi"}), start=1):
        pub_id = raw_doc.get("pub_id", "unknown")

        try:
            unified_doc = process_one_publikasi(raw_doc)
        except Exception as e:
            # exc_info=True untuk full traceback — penting saat debugging
            logger.error(
                f"Gagal memproses pub_id={pub_id}. Error: {e}",
                exc_info=True,
            )
            total_skip += 1
            continue

        if unified_doc is None:
            logger.info(f"[{i}/{total_raw}] Skip: pub_id={pub_id}")
            total_skip += 1
            continue

        # Append per dokumen — jaga batch size tetap terkontrol
        batch.append(unified_doc)
        if len(batch) >= BATCH_SIZE:
            saved = save_documents(batch, doc_coll)
            total_saved += saved
            logger.info(f"  [{i}/{total_raw}] Batch flushed: {saved} dokumen disimpan")
            batch = []

        logger.info(f"[{i}/{total_raw}] OK: pub_id={pub_id}")

    # Flush sisa batch
    if batch:
        saved = save_documents(batch, doc_coll)
        total_saved += saved
        logger.info(f"Final batch flushed: {saved} dokumen disimpan")

    # --- Summary ---
    logger.info("=" * 50)
    logger.info("PREPROCESSING PUBLIKASI SELESAI")
    logger.info(f"  Total raw diproses : {total_raw}")
    logger.info(f"  Dokumen dihasilkan : {total_saved}")
    logger.info(f"  Dilewati           : {total_skip}")
    logger.info("=" * 50)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run()
