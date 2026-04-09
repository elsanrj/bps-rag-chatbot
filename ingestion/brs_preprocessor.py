"""
brs_preprocessor.py
Fase 1B - Hari 11: Preprocessing Data BRS → Unified Document Schema

Tanggung jawab modul ini:
  - Baca raw_docs (source_type: brs) dari MongoDB
  - Deteksi tipe abstract: Normal (A), Redirect (B), atau Nonexist (C)
  - Resolusi per tipe:
      Tipe A → strip HTML, gabungkan dengan title
      Tipe B → scrape halaman eksternal via href "Selanjutnya", update pdf_url
      Tipe C → content = title + catatan fallback untuk LLM
  - Deteksi region dari content: Kota Bandung / Jawa Barat / Nasional
  - Simpan hasil ke MongoDB collection `documents` (unified schema)

Tipe Abstract BRS:
  Tipe A (Normal)  : Berisi teks abstrak utuh (HTML berpoin atau paragraf)
  Tipe B (Redirect): Hanya berisi tag <a href="...">Selanjutnya</a>
                     → scrape halaman target untuk ambil abstrak + pdf_url baru
  Tipe C (Nonexist): Kosong, "-", "tidak ada abstrak", atau null
                     → content = title + catatan fallback

Catatan pengembangan:
  Tahap ini adalah Skenario 1 (dev awal) — tanpa ekstraksi PDF.
  pdf_url disimpan di metadata.extra (diperbarui jika Tipe B berhasil di-scrape)
  untuk digunakan di Skenario 2 (Hari 11 fase lanjutan).

Prasyarat:
  - brs_ingestor.py sudah dijalankan (raw_docs terisi)
  - cleaner.py tersedia di direktori yang sama
  - pip install beautifulsoup4 requests

Jalankan:
  python -m ingestion.brs_preprocessor

Env variables yang diperlukan (.env):
  MONGODB_URI : Connection string MongoDB Atlas
  MONGODB_DB  : Nama database (contoh: bps_rag)
"""

import os
import re
import time
import logging
import requests
import cloudscraper

from datetime import datetime
from urllib.parse import urlparse
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
from bs4 import BeautifulSoup
from html import unescape

from ingestion.cleaner import (
    clean_html,
    normalize_whitespace,
    validate_doc,
    generate_id,
)

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

MONGODB_URI    = os.getenv("MONGODB_URI")
MONGODB_DB     = os.getenv("MONGODB_DB", "bps_rag")
RAW_COLL       = "raw_docs"
DOC_COLL       = "documents"
BATCH_SIZE     = 100

SCRAPE_DELAY   = 1.0   # detik antar request scraping — jangan membebani server eksternal
SCRAPE_TIMEOUT = 10    # timeout request scraping

# Nilai abstrak yang dianggap Tipe C (Nonexist)
_NONEXIST_VALUES = {"", "-", "tidak ada abstrak", None}

# Selector CSS yang umum digunakan BPS untuk konten abstrak
# Dicoba berurutan — ambil yang pertama berhasil
_BPS_ABSTRACT_SELECTORS = [
    "div.Abstract_abstract__KyuKP",
    "div.abstract-content",
    "div.abstrak",
    "div#abstract",
    "div.content-brs",
    "div.isi-abstrak",
    "article p",           # fallback: paragraf pertama di article
]

# Selector untuk link PDF di halaman BPS
_BPS_PDF_SELECTORS = [
    "a.download-product",
    "a[href*='.pdf']",
    "a.download-pdf",
    "a[title*='PDF']",
    "a[title*='pdf']",
]

# Teks link yang mengindikasikan Tipe B (redirect)
# Ditemukan di data aktual: "Selengkapnya" bukan "Selanjutnya"
_REDIRECT_LINK_TEXTS = ["selanjutnya", "selengkapnya", "lanjut", "lihat selengkapnya"]

# Tipe abstract sebagai konstanta
ABSTRACT_TYPE_A = "normal"
ABSTRACT_TYPE_B = "redirect"
ABSTRACT_TYPE_C = "nonexist"


# ---------------------------------------------------------------------------
# Deteksi tipe abstract
# ---------------------------------------------------------------------------

def detect_abstract_type(abstract: str) -> tuple[str, str | None]:
    """
    Deteksi tipe abstract BRS menggunakan BeautifulSoup.

    Return tuple (tipe, redirect_url):
      - (ABSTRACT_TYPE_A, None)  → abstrak normal, siap diproses
      - (ABSTRACT_TYPE_B, url)   → redirect, url = href dari tag <a>
      - (ABSTRACT_TYPE_C, None)  → kosong/nonexist

    Logika deteksi:
      1. Cek apakah kosong / nilai nonexist → Tipe C
      2. Parse HTML dengan BeautifulSoup
      3. Cek apakah satu-satunya konten bermakna adalah tag <a> dengan
         teks "Selanjutnya" atau "Lanjut" → Tipe B, ambil href
      4. Selain itu → Tipe A
    """
    if not abstract:
        return ABSTRACT_TYPE_C, None

    stripped = unescape(abstract.strip())
    if stripped.lower() in _NONEXIST_VALUES:
        return ABSTRACT_TYPE_C, None

    soup = BeautifulSoup(stripped, "html.parser")

    # Cek apakah visible text kosong setelah parsing
    visible_text = soup.get_text(separator=" ").strip()
    if not visible_text or visible_text.lower() in _NONEXIST_VALUES:
        return ABSTRACT_TYPE_C, None

    # Cek apakah elemen utamanya adalah tag <a> dengan teks "Selanjutnya"
    a_tags = soup.find_all("a")
    if a_tags:
        for a in a_tags:
            a_text = a.get_text(strip=True).lower()
            if any(keyword in a_text for keyword in _REDIRECT_LINK_TEXTS):
                href = a.get("href", "").strip()
                if href:
                    # Verifikasi tidak ada konten lain yang bermakna di luar tag <a>
                    non_a_text = visible_text
                    for a_tag in a_tags:
                        non_a_text = non_a_text.replace(a_tag.get_text(strip=True), "")
                    if not non_a_text.strip():
                        return ABSTRACT_TYPE_B, href

    return ABSTRACT_TYPE_A, None


# ---------------------------------------------------------------------------
# Konversi Tipe A — abstrak normal
# ---------------------------------------------------------------------------

def convert_type_a(abstract: str) -> str:
    """
    Konversi abstrak normal (Tipe A) menjadi teks bersih.
    
    Handle semua format yang ditemukan di data aktual BRS:
      - <div>/<span> dengan inline style  → ambil teks langsung
      - <p> paragraf                       → gabung per paragraf
      - <ul><li> berpoin                   → tiap li jadi kalimat
      - <h3> sebagai prefix keterangan     → disertakan sebagai konteks
      - Kombinasi format di atas           → diproses sesuai tag masing-masing
    
    HTML-escaped characters (&lt; dst) di-unescape sebelum parsing.
    """

    # Unescape dulu — data aktual BRS menggunakan HTML escaping
    unescaped = unescape(abstract)
    soup = BeautifulSoup(unescaped, "html.parser")

    # Hapus tag yang tidak berkontribusi pada konten
    for tag in soup.find_all(["script", "style", "nav", "footer", "header"]):
        tag.decompose()

    parts = []

    # Iterasi child langsung dari root — proses sesuai tag
    # Ini lebih robust daripada find_all karena menghormati urutan konten
    for element in soup.find_all(["h3", "p", "ul", "ol", "div", "span"]):
        # Skip elemen yang sudah diproses sebagai bagian dari parent
        if element.find_parent(["ul", "ol"]):
            continue

        tag_name = element.name

        if tag_name == "h3":
            # H3 biasanya keterangan/judul bagian — sertakan sebagai konteks
            text = normalize_whitespace(element.get_text(separator=" "))
            if text:
                parts.append(text)

        elif tag_name in ("ul", "ol"):
            # List — tiap <li> jadi kalimat terpisah
            for li in element.find_all("li"):
                text = normalize_whitespace(li.get_text(separator=" "))
                if not text:
                    continue
                if not text.endswith("."):
                    text += "."
                parts.append(text)

        elif tag_name == "p":
            text = normalize_whitespace(element.get_text(separator=" "))
            if text:
                if not text.endswith("."):
                    text += "."
                parts.append(text)

        elif tag_name in ("div", "span"):
            # Hanya proses div/span yang tidak punya child block-level
            # (untuk menghindari duplikasi dengan child yang sudah diproses)
            has_block_child = any(
                c.name in ("p", "ul", "ol", "div", "h3")
                for c in element.children
                if hasattr(c, "name") and c.name
            )
            if not has_block_child:
                text = normalize_whitespace(element.get_text(separator=" "))
                if text:
                    if not text.endswith("."):
                        text += "."
                    parts.append(text)

    # Deduplikasi — urutan dipertahankan (bisa ada duplikat karena nested tags)
    seen = set()
    unique_parts = []
    for part in parts:
        if part not in seen:
            seen.add(part)
            unique_parts.append(part)

    if unique_parts:
        return normalize_whitespace(" ".join(unique_parts))

    # Fallback: ambil semua teks jika tidak ada struktur yang dikenali
    return normalize_whitespace(soup.get_text(separator=" "))

# ---------------------------------------------------------------------------
# Resolusi Tipe B — scrape halaman eksternal
# ---------------------------------------------------------------------------

def scrape_external_abstract(url: str) -> tuple[str, str | None]:
    """
    Scrape halaman BPS eksternal untuk mengambil teks abstrak dan URL PDF.

    Mencoba beberapa selector CSS umum yang digunakan BPS secara berurutan.
    Jika tidak ada yang cocok, fallback ke paragraf terpanjang di halaman.

    Parameter:
      url : URL halaman eksternal dari href tag <a> "Selanjutnya"

    Return tuple (abstract_text, pdf_url):
      - abstract_text : teks abstrak hasil scrape, atau "" jika gagal
      - pdf_url       : URL PDF yang ditemukan di halaman, atau None
    """
    try:
        # 1. Buat mesin scraper yang meniru browser asli
        scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True
            }
        )

        # 2. Lakukan request menggunakan scraper (bukan requests lagi)
        response = scraper.get(url, timeout=SCRAPE_TIMEOUT)
        response.raise_for_status()

    except Exception as e: # Tangkap semua error scraper
        logger.warning(f"Gagal scrape URL={url}: {e}")
        return "", None

    soup = BeautifulSoup(response.text, "html.parser")

    # --- Cari abstrak ---
    abstract_text = ""
    for selector in _BPS_ABSTRACT_SELECTORS:
        element = soup.select_one(selector)
        if element:
            # Gunakan convert_type_a karena format di dalam selector
            # sama dengan format abstract Tipe A (div/span/p/ul/li/h3)
            text = convert_type_a(str(element))
            if text and len(text) > 50:
                abstract_text = text
                logger.info(f"  Abstrak ditemukan via selector '{selector}': {len(text)} karakter")
                break

    # --- Cari URL PDF ---
    pdf_url = None
    for selector in _BPS_PDF_SELECTORS:
        pdf_element = soup.select_one(selector)
        if pdf_element:
            href = pdf_element.get("href", "").strip()
            if href:
                # Lengkapi URL relatif menjadi absolut
                if href.startswith("/"):
                    parsed = urlparse(url)
                    href = f"{parsed.scheme}://{parsed.netloc}{href}"
                pdf_url = href
                logger.info(f"  PDF URL ditemukan via selector '{selector}': {pdf_url[:80]}...")
                break

    return abstract_text, pdf_url


# ---------------------------------------------------------------------------
# Region resolver khusus BRS
# ---------------------------------------------------------------------------

# Keyword untuk deteksi region dari content BRS
# BRS bisa berskala kota, provinsi, atau nasional
_REGION_KEYWORDS_NASIONAL = [
    "indonesia", "nasional", "seluruh indonesia",
    "tingkat nasional", "skala nasional",
]
_REGION_KEYWORDS_JABAR = [
    "jawa barat", "jabar", "provinsi jawa barat",
]
# Kota Bandung adalah default — tidak perlu keyword khusus


def resolve_region_brs(content: str) -> str:
    """
    Deteksi region dari content BRS.
    Cakupan BRS domain 3273 bisa berupa:
      - Kota Bandung (default)
      - Jawa Barat (jika menyebut nama provinsi)
      - Nasional (jika menyebut Indonesia/nasional)

    Prioritas: Nasional > Jawa Barat > Kota Bandung
    Alasan: jika content menyebut "Indonesia" sekaligus "Jawa Barat",
    cakupan yang lebih luas lebih tepat digunakan sebagai region.

    BRS tidak sampai ke level kecamatan/kelurahan sehingga tidak
    menggunakan REGION_MAP dari regions.py.
    """
    content_lower = content.lower()
    
    for keyword in _REGION_KEYWORDS_NASIONAL:
        if keyword in content_lower:
            return "Nasional"

    for keyword in _REGION_KEYWORDS_JABAR:
        if keyword in content_lower:
            return "Jawa Barat"

    return "Kota Bandung"


# ---------------------------------------------------------------------------
# Build content
# ---------------------------------------------------------------------------

def build_content(
    title: str,
    abstract: str,
) -> tuple[str, str, str | None]:
    """
    Deteksi tipe abstract, resolusi, dan bangun content final.

    Return tuple (content, abstract_type, resolved_pdf_url):
      - content          : teks final siap embedding
      - abstract_type    : ABSTRACT_TYPE_A / B / C (untuk metadata audit)
      - resolved_pdf_url : URL PDF baru dari scraping (Tipe B), atau None
    """
    clean_title = normalize_whitespace(clean_html(title))
    abstract_type, redirect_url = detect_abstract_type(abstract)
    resolved_pdf_url = None

    # --- Tipe A: abstrak normal ---
    if abstract_type == ABSTRACT_TYPE_A:
        clean_abstract = convert_type_a(abstract)
        if clean_abstract:
            content = f"{clean_title}. {clean_abstract}" if clean_title else clean_abstract
            return normalize_whitespace(content), ABSTRACT_TYPE_A, None
        # Konversi menghasilkan kosong — turunkan ke Tipe C
        abstract_type = ABSTRACT_TYPE_C

    # --- Tipe B: redirect ke halaman eksternal ---
    if abstract_type == ABSTRACT_TYPE_B and redirect_url:
        logger.info(f"  Tipe B terdeteksi, scrape: {redirect_url}")
        time.sleep(SCRAPE_DELAY)
        scraped_text, resolved_pdf_url = scrape_external_abstract(redirect_url)

        if scraped_text:
            content = f"{clean_title}. {scraped_text}" if clean_title else scraped_text
            return normalize_whitespace(content), ABSTRACT_TYPE_B, resolved_pdf_url

        # Scraping gagal — turunkan ke Tipe C
        logger.warning(f"  Scraping gagal untuk {redirect_url}, fallback ke Tipe C")
        abstract_type = ABSTRACT_TYPE_C

    # --- Tipe C: nonexist / fallback ---
    # Content minimal: title + catatan untuk LLM agar tidak null
    if clean_title:
        fallback = (
            f"{clean_title}. "
            "Berita Resmi Statistik ini tidak memiliki abstrak yang tersedia. "
            "Informasi detail dapat dilihat pada dokumen PDF terkait."
        )
        return normalize_whitespace(fallback), ABSTRACT_TYPE_C, resolved_pdf_url

    # Title juga kosong — tidak ada yang bisa disimpan
    return "", ABSTRACT_TYPE_C, None


# ---------------------------------------------------------------------------
# Proses satu raw BRS
# ---------------------------------------------------------------------------

def process_one_brs(raw_doc: dict) -> dict | None:
    """
    Proses satu raw BRS dari MongoDB menjadi satu unified document.
    BRS menghasilkan tepat satu dokumen per record (1 abstract = 1 chunk).

    Parameter:
      raw_doc : Satu dokumen dari collection raw_docs (source_type: brs)

    Return unified document dict atau None jika tidak valid.
    """
    brs_id     = raw_doc.get("brs_id")
    source_url = raw_doc.get("source_url")

    # Verifikasi struktur payload sebelum akses field — pelajaran dari dynamic data
    payload = raw_doc.get("payload", {})

    if not payload:
        logger.warning(f"Payload kosong untuk brs_id={brs_id}")
        return None

    # Akses field langsung dari payload — tidak ada wrapper "data"
    title    = payload.get("title", "")
    abstract = payload.get("abstract", "") or ""
    subcsa   = payload.get("subcsa", "")
    rl_date  = payload.get("rl_date", "")
    pdf_url  = payload.get("pdf", "")

    # Field yang diabaikan: thumbnail, slide, size, subj_id, subcsa_id

    content, abstract_type, resolved_pdf_url = build_content(title, abstract)

    if not content:
        logger.debug(f"Content kosong untuk brs_id={brs_id} (title juga kosong), skip.")
        return None

    candidate = {"content": content, "id": generate_id(content)}
    if not validate_doc(candidate):
        logger.debug(f"Validasi gagal untuk brs_id={brs_id}")
        return None

    # Gunakan pdf_url yang diperbarui dari scraping (Tipe B) jika ada
    final_pdf_url = resolved_pdf_url or pdf_url or None

    # Deteksi region dari title — khusus BRS: Kota Bandung / Jawa Barat / Nasional
    region = resolve_region_brs(title)

    return {
        "id"          : candidate["id"],
        "source_type" : "brs",
        "title"       : normalize_whitespace(clean_html(title)),
        "content"     : content,
        "metadata"    : {
            "date"       : rl_date or None,
            "year"       : int(rl_date[:4]) if rl_date and rl_date[:4].isdigit() else None,
            "category"   : subcsa,
            "region"     : region,
            "source_url" : source_url,
            "extra"      : {
                "brs_id"        : brs_id,
                "abstract_type" : abstract_type,    # A/B/C — untuk audit kualitas data
                "pdf_url"       : final_pdf_url,    # URL PDF final (diperbarui jika Tipe B)
                "pdf_extracted" : False,            # akan diubah True di Skenario 2
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
    Pipeline utama preprocessing BRS:
      1. Iterasi semua raw_docs (source_type: brs)
      2. Deteksi tipe abstract → resolusi → build content
      3. Deteksi region dari content
      4. Upsert ke collection `documents`
    """
    if not MONGODB_URI:
        logger.error("MONGODB_URI tidak ditemukan di environment. Hentikan.")
        return

    client   = MongoClient(MONGODB_URI)
    db       = client[MONGODB_DB]
    raw_coll = db[RAW_COLL]
    doc_coll = db[DOC_COLL]

    logger.info(f"Terhubung ke MongoDB: {MONGODB_DB}")

    total_raw    = raw_coll.count_documents({"source_type": "brs"})
    total_saved  = 0
    total_skip   = 0
    count_type_a = 0
    count_type_b = 0
    count_type_c = 0
    batch        = []

    logger.info(f"Total raw BRS yang akan diproses: {total_raw}")

    for i, raw_doc in enumerate(raw_coll.find({"source_type": "brs"}), start=1):
        brs_id = raw_doc.get("brs_id", "unknown")

        try:
            unified_doc = process_one_brs(raw_doc)
        except Exception as e:
            logger.error(
                f"Gagal memproses brs_id={brs_id}. Error: {e}",
                exc_info=True,
            )
            total_skip += 1
            continue

        if unified_doc is None:
            logger.debug(f"[{i}/{total_raw}] Skip: brs_id={brs_id}")
            total_skip += 1
            continue

        # Track tipe abstract untuk statistik
        atype = unified_doc["metadata"]["extra"].get("abstract_type", "")
        if atype == ABSTRACT_TYPE_A:
            count_type_a += 1
        elif atype == ABSTRACT_TYPE_B:
            count_type_b += 1
        elif atype == ABSTRACT_TYPE_C:
            count_type_c += 1

        # Append per dokumen — jaga batch size tetap terkontrol
        batch.append(unified_doc)
        if len(batch) >= BATCH_SIZE:
            saved = save_documents(batch, doc_coll)
            total_saved += saved
            logger.info(f"  [{i}/{total_raw}] Batch flushed: {saved} dokumen disimpan")
            batch = []

        logger.info(
            f"[{i}/{total_raw}] OK: brs_id={brs_id} "
            f"| tipe={atype} | region={unified_doc['metadata']['region']}"
        )

    # Flush sisa batch
    if batch:
        saved = save_documents(batch, doc_coll)
        total_saved += saved
        logger.info(f"Final batch flushed: {saved} dokumen disimpan")

    # --- Summary ---
    logger.info("=" * 50)
    logger.info("PREPROCESSING BRS SELESAI")
    logger.info(f"  Total raw diproses : {total_raw}")
    logger.info(f"  Dokumen dihasilkan : {total_saved}")
    logger.info(f"  Dilewati           : {total_skip}")
    logger.info(f"  Tipe A (Normal)    : {count_type_a}")
    logger.info(f"  Tipe B (Redirect)  : {count_type_b}")
    logger.info(f"  Tipe C (Nonexist)  : {count_type_c}")
    logger.info("=" * 50)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run()