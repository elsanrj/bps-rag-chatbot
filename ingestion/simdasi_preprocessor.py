"""
simdasi_preprocessor.py
Fase 1B: Preprocessing Data SIMDASI → Unified Document Schema

Tanggung jawab modul ini:
  - Baca raw_docs (source_type: simdasi) dari MongoDB
  - Parse field `kolom` sebagai definisi variabel per kolom
  - Konversi setiap baris data menjadi kalimat deskriptif dalam Bahasa Indonesia
  - Handle simbol BPS (…, –, NA, dll) via apply_symbol_legend dari cleaner.py
  - Simpan hasil ke MongoDB collection `documents` (unified schema)

Strategi chunking SIMDASI:
  1 tabel × 1 tahun = 1 raw_doc → bisa menghasilkan beberapa unified_doc
  Karena setiap baris tabel = satu fakta statistik yang berdiri sendiri,
  setiap baris dikonversi menjadi satu dokumen terpisah (atomic, no chunking).

Prasyarat:
  - simdasi_ingestor.py sudah dijalankan (raw_docs terisi)
  - cleaner.py tersedia di direktori yang sama

Jalankan:
  python -m ingestion.simdasi_preprocessor

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
    apply_symbol_legend,
    validate_doc,
    generate_id,
)
from ingestion.regions import VILLAGES, DISTRICTS

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


# ---------------------------------------------------------------------------
# Region resolver (sama dengan dynamic_preprocessor)
# ---------------------------------------------------------------------------

def resolve_region(title: str, row_label: str, lingkup: str) -> str:
    """
    Cari nama kecamatan atau kelurahan dalam title secara case-insensitive.
    Return nama wilayah jika ditemukan, default 'Kota Bandung' jika tidak.
    Prioritas: nama terpanjang dicek dulu untuk hindari partial match.
    """
    title_low = title.lower()
    row_low = row_label.lower() if row_label else ""
    lingkup_low = lingkup.lower() if lingkup else ""
    
    # Strategi: Urutkan kunci berdasarkan panjang karakter (descending) 
    # agar nama panjang tidak terpotong (misal: "Bandung Kidul" vs "Bandung")
    sorted_villages = sorted(VILLAGES.items(), key=len, reverse=True)
    sorted_districts = sorted(DISTRICTS.items(), key=len, reverse=True)

    # 1. Cek di row_label dengan bantuan hint 'lingkup'
    # Jika lingkup menyebutkan kecamatan, cari di map kecamatan dulu            
    if "kecamatan" in lingkup_low:
        for name, display in sorted_districts:
            if name in row_low: return display
            
        for name, display in sorted_villages:
            if name in row_low: return display
            
    else:
        for name, display in sorted_villages:
            if name in row_low: return display
        
        for name, display in sorted_districts:
            if name in row_low: return display

    # LANGKAH 2: Cek di Judul (Title)
    for name, display in sorted_villages:
        if name in title_low: return display

    for name, display in sorted_districts:
        if name in title_low: return display
        
    return "Kota Bandung"


# ---------------------------------------------------------------------------
# Parse kolom definitions
# ---------------------------------------------------------------------------

def parse_kolom(kolom: dict) -> dict:
    """
    Parse field `kolom` dari payload SIMDASI menjadi lookup map:
      {kolom_key → nama_variabel}

    Field `kolom` adalah dict dengan key acak (misal "hs2vnuacgs") yang
    masing-masing berisi `nama_variabel` dan metadata lainnya.

    Contoh input:
      {
        "hs2vnuacgs": {"nama_variabel": "PNS (Laki-Laki)", ...},
        "ndnfgelumn": {"nama_variabel": "PNS (Perempuan)", ...},
      }

    Return:
      {"hs2vnuacgs": "PNS (Laki-Laki)", "ndnfgelumn": "PNS (Perempuan)"}
    """
    if not isinstance(kolom, dict):
        return {}

    result = {}
    for key, col_def in kolom.items():
        if not isinstance(col_def, dict):
            continue
        nama = col_def.get("nama_variabel")
        if nama:
            result[key] = clean_html(nama)   # nama_variabel kadang mengandung HTML

    return result


# ---------------------------------------------------------------------------
# Konversi satu baris data menjadi kalimat
# ---------------------------------------------------------------------------

def build_row_sentence(
    judul: str,
    lingkup: str,
    row_label: str,
    kolom_map: dict,
    variables: dict,
    satuan: str | None,
    keterangan_data: dict,
) -> str:
    """
    Konversi satu baris data tabel SIMDASI menjadi kalimat deskriptif.

    Template:
      {judul}.
      {lingkup} {row_label}: {kolom1} sebesar {val1}{satuan},
      {kolom2} sebesar {val2}{satuan}.

    Contoh output:
      Jumlah Pegawai Negeri Sipil Menurut Jabatan dan Jenis Kelamin, 2018.
      Jabatan Fungsional Tertentu: PNS Laki-Laki sebesar 3.937 orang,
      PNS Perempuan sebesar 8.126 orang, total 12.063 orang.

    Nilai yang tidak bermakna (simbol BPS) di-handle via apply_symbol_legend.
    Baris yang semua nilainya tidak bermakna akan return string kosong.
    """
    parts = []
    has_meaningful_value = False

    for col_key, col_name in kolom_map.items():
        var_data = variables.get(col_key, {})
        if not isinstance(var_data, dict):
            continue

        # Ambil value — gunakan value_raw jika ada, fallback ke value
        raw_val = var_data.get("value_raw") or var_data.get("value")
        value_code = var_data.get("value_code")

        # Jika ada value_code, gunakan itu untuk lookup simbol
        # value_code biasanya berisi simbol seperti "–", "...", "NA"
        lookup_val = value_code if value_code else str(raw_val) if raw_val is not None else None

        cleaned = apply_symbol_legend(lookup_val, legend=keterangan_data)

        # Skip nilai yang tidak bermakna
        if cleaned in ("data tidak tersedia", "tidak dapat ditampilkan"):
            continue

        has_meaningful_value = True

        satuan_part = f" {satuan}" if satuan and satuan.strip() else ""
        parts.append(f"{col_name} sebesar {cleaned}{satuan_part}")

    if not has_meaningful_value or not parts:
        return None

    # Bersihkan judul dari HTML jika ada
    clean_judul  = clean_html(judul)
    clean_lingkup = clean_html(lingkup) if lingkup else ""
    clean_label  = clean_html(row_label)

    # Header: judul + tahun
    header = f"{clean_judul}"

    # Prefix baris: gunakan lingkup jika ada
    prefix = f"{clean_lingkup} {clean_label}" if clean_lingkup else clean_label

    sentence = f"{header}. {prefix}: {', '.join(parts)}"
    return normalize_whitespace(sentence)


# ---------------------------------------------------------------------------
# Proses satu raw_doc SIMDASI
# ---------------------------------------------------------------------------

def process_one_table(raw_doc: dict) -> list[dict]:
    """
    Proses satu raw SIMDASI dokumen dari MongoDB menjadi list unified documents.
    Setiap baris data menghasilkan satu dokumen terpisah.

    Parameter:
      raw_doc : Satu dokumen dari collection raw_docs (source_type: simdasi)

    Return list of unified document dicts (siap upsert ke collection `documents`).
    """
    # Ambil field konteks dari raw_doc (bukan dari payload)
    id_tabel   = raw_doc.get("id_tabel")
    tahun      = raw_doc.get("tahun")
    mms_id     = raw_doc.get("mms_id")
    source_url = raw_doc.get("source_url")

    # Payload adalah root response dari API SIMDASI /id/25
    # Verifikasi dulu field-field yang akan diakses
    payload = raw_doc.get("payload", {})

    if not payload:
        logger.warning(f"Payload kosong untuk id_tabel={id_tabel} tahun={tahun}")
        return []

    # Field-field utama dari payload
    judul           = clean_html(payload.get("judul_tabel"))
    lingkup         = clean_html(payload.get("lingkup_id"))
    tahun_data      = payload.get("tahun_data", tahun)
    bab             = payload.get("bab")
    subject         = payload.get("subject")
    sumber          = clean_html(payload.get("sumber"))
    keterangan_data = payload.get("keterangan_data", {})
    kolom           = payload.get("kolom", {})
    data_rows       = payload.get("data", [])

    if not judul:
        logger.warning(f"judul_tabel kosong untuk id_tabel={id_tabel} tahun={tahun}")
        return []

    if not isinstance(data_rows, list) or not data_rows:
        logger.debug(f"data rows kosong untuk id_tabel={id_tabel} tahun={tahun}")
        return []

    if not isinstance(kolom, dict) or not kolom:
        logger.warning(f"kolom kosong untuk id_tabel={id_tabel} tahun={tahun}")
        return []

    # Build kolom map: {key → nama_variabel}
    kolom_map = parse_kolom(kolom)
    if not kolom_map:
        logger.warning(f"kolom_map kosong setelah parse untuk id_tabel={id_tabel}")
        return []

    unified_docs = []

    for row in data_rows:
        if not isinstance(row, dict):
            continue

        row_label  = row.get("label_raw") or row.get("label")
        variables  = row.get("variables", {})
        satuan     = row.get("satuan")   # bisa None

        if not row_label or not isinstance(variables, dict):
            continue

        content = build_row_sentence(
            judul           = judul,
            lingkup         = lingkup,
            row_label       = row_label,
            kolom_map       = kolom_map,
            variables       = variables,
            satuan          = satuan,
            keterangan_data = keterangan_data,
        )

        if not content:
            continue

        candidate = {"content": content, "id": generate_id(content)}
        if not validate_doc(candidate):
            continue

        unified_doc = {
            "id"          : candidate["id"],
            "source_type" : "simdasi",
            "title"       : judul,
            "content"     : content,
            "metadata"    : {
                "date"       : None,
                "year"       : int(tahun_data) if str(tahun_data).isdigit() else None,
                "category"   : subject or bab,
                "region"     : resolve_region(judul, row_label, lingkup),
                "source_url" : source_url,
                "extra"      : {
                    "id_tabel"  : id_tabel,
                    "mms_id"    : mms_id,
                    "row_label" : row_label,
                    "sumber"    : sumber,
                },
            },
            "processed_at" : datetime.utcnow(),
        }

        unified_docs.append(unified_doc)

    return unified_docs


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
    Pipeline utama preprocessing SIMDASI:
      1. Iterasi semua raw_docs (source_type: simdasi)
      2. Parse kolom + konversi baris → unified documents
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

    total_raw   = raw_coll.count_documents({"source_type": "simdasi"})
    total_saved = 0
    total_skip  = 0
    batch       = []

    logger.info(f"Total raw SIMDASI yang akan diproses: {total_raw}")

    for i, raw_doc in enumerate(raw_coll.find({"source_type": "simdasi"}), start=1):
        id_tabel = raw_doc.get("id_tabel", "unknown")
        tahun    = raw_doc.get("tahun", "unknown")

        try:
            unified_docs = process_one_table(raw_doc)
        except Exception as e:
            # exc_info=True mencetak full traceback — penting untuk debugging
            logger.error(
                f"Gagal memproses id_tabel={id_tabel} tahun={tahun}. Error: {e}",
                exc_info=True,
            )
            total_skip += 1
            continue

        if not unified_docs:
            logger.debug(f"[{i}/{total_raw}] Tidak ada dokumen dihasilkan: id_tabel={id_tabel} tahun={tahun}")
            total_skip += 1
            continue

        # Append per dokumen — jaga batch size tetap terkontrol
        for doc in unified_docs:
            batch.append(doc)
            if len(batch) >= BATCH_SIZE:
                saved = save_documents(batch, doc_coll)
                total_saved += saved
                logger.info(f"  [{i}/{total_raw}] Batch flushed: {saved} dokumen disimpan")
                batch = []

        logger.info(f"[{i}/{total_raw}] OK: id_tabel={id_tabel} tahun={tahun} → {len(unified_docs)} dokumen")

    # Flush sisa batch
    if batch:
        saved = save_documents(batch, doc_coll)
        total_saved += saved
        logger.info(f"Final batch flushed: {saved} dokumen disimpan")

    # --- Summary ---
    logger.info("=" * 50)
    logger.info("PREPROCESSING SIMDASI SELESAI")
    logger.info(f"  Total raw diproses : {total_raw}")
    logger.info(f"  Dokumen dihasilkan : {total_saved}")
    logger.info(f"  Dilewati           : {total_skip}")
    logger.info("=" * 50)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run()