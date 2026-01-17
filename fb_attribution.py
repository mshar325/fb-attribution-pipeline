import os
import time
import threading
import requests
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import HTTPServer, BaseHTTPRequestHandler

# =====================================================
# CONFIG
# =====================================================

GRAPH = "https://graph.facebook.com/v19.0"

TOKENS = [
    ("FB_TOKEN_1", os.environ["FB_TOKEN_1"]),
    ("FB_TOKEN_2", os.environ["FB_TOKEN_2"]),
    ("FB_TOKEN_3", os.environ["FB_TOKEN_3"]),
]

POSTGRES_URI = os.environ["POSTGRES_URI"]

MAX_WORKERS = 12
BATCH_SIZE = 500
ADVISORY_LOCK_ID = 777001

SLEEP_SECONDS = int(os.getenv("SLEEP_SECONDS", "300"))
HEALTH_PORT = 10001

# =====================================================
# GLOBAL CACHES
# =====================================================

meta_cache = {}
failed_ids = set()

# =====================================================
# HEALTH SERVER (RENDER)
# =====================================================

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, *_):
        return

def start_health_server():
    server = HTTPServer(("0.0.0.0", HEALTH_PORT), HealthHandler)
    print(f"🌐 Health server listening on port {HEALTH_PORT}")
    server.serve_forever()

# =====================================================
# META RESOLUTION (AD → ADSET)
# =====================================================

def resolve_meta_object(object_id):
    if not object_id or object_id in failed_ids:
        return None
    if object_id in meta_cache:
        return meta_cache[object_id]

    for token_name, token in TOKENS:
        try:
            # ---- TRY AD ----
            ad_r = requests.get(
                f"{GRAPH}/{object_id}",
                params={"access_token": token, "fields": "name,adset_id,campaign_id"},
                timeout=10
            )

            if ad_r.status_code == 200:
                ad = ad_r.json()
                if ad.get("adset_id") and ad.get("campaign_id"):
                    adset = requests.get(
                        f"{GRAPH}/{ad['adset_id']}",
                        params={"access_token": token, "fields": "name"},
                        timeout=10
                    ).json()

                    camp = requests.get(
                        f"{GRAPH}/{ad['campaign_id']}",
                        params={"access_token": token, "fields": "name,objective"},
                        timeout=10
                    ).json()

                    meta_cache[object_id] = {
                        "level": "AD",
                        "fb_ad_id": object_id,
                        "fb_ad_name": ad.get("name"),
                        "fb_adset_id": ad["adset_id"],
                        "fb_adset_name": adset.get("name"),
                        "fb_campaign_id": ad["campaign_id"],
                        "fb_campaign_name": camp.get("name"),
                        "fb_campaign_objective": camp.get("objective"),
                        "resolved_with_token": token_name,
                    }
                    return meta_cache[object_id]

            # ---- TRY ADSET ----
            adset_r = requests.get(
                f"{GRAPH}/{object_id}",
                params={"access_token": token, "fields": "name,campaign_id"},
                timeout=10
            )

            if adset_r.status_code == 200:
                adset = adset_r.json()
                camp = requests.get(
                    f"{GRAPH}/{adset['campaign_id']}",
                    params={"access_token": token, "fields": "name,objective"},
                    timeout=10
                ).json()

                meta_cache[object_id] = {
                    "level": "ADSET",
                    "fb_ad_id": None,
                    "fb_ad_name": None,
                    "fb_adset_id": object_id,
                    "fb_adset_name": adset.get("name"),
                    "fb_campaign_id": adset["campaign_id"],
                    "fb_campaign_name": camp.get("name"),
                    "fb_campaign_objective": camp.get("objective"),
                    "resolved_with_token": token_name,
                }
                return meta_cache[object_id]

        except Exception:
            continue

    failed_ids.add(object_id)
    return None

# =====================================================
# MAIN WORKER CYCLE
# =====================================================

def run_cycle():
    conn = psycopg2.connect(POSTGRES_URI)
    cur = conn.cursor()

    try:
        cur.execute("SET statement_timeout = 0;")
        cur.execute("SELECT pg_try_advisory_lock(%s);", (ADVISORY_LOCK_ID,))
        if not cur.fetchone()[0]:
            print("⛔ Job already running")
            return
        conn.commit()

        # -------------------------------
        # PHASE 1: BACKFILL NEW ORDERS
        # -------------------------------
        print("📥 Backfilling new Shopify rows...")

        cur.execute("""
        SELECT s.row_id, s.utm_source, s.utm_campaign, s.utm_content, s.utm_term
        FROM shopify_orders_marketing s
        LEFT JOIN shopify_facebook_attribution f
          ON s.row_id = f.row_id
        WHERE LOWER(s.utm_source) IN ('facebook','fb','ig','instagram')
          AND f.row_id IS NULL
        LIMIT 1000
        """)

        new_rows = cur.fetchall()

        if new_rows:
            ids = {r[4] for r in new_rows if r[4]}
            with ThreadPoolExecutor(MAX_WORKERS) as ex:
                list(as_completed([ex.submit(resolve_meta_object, i) for i in ids]))

            insert_sql = """
            INSERT INTO shopify_facebook_attribution (
              row_id, utm_source, utm_campaign, utm_content, utm_term,
              fb_ad_id, fb_ad_name, fb_adset_id, fb_adset_name,
              fb_campaign_id, fb_campaign_name, fb_campaign_objective,
              resolved_with_token, resolution_status, resolved_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """

            batch = []
            for r in new_rows:
                h = meta_cache.get(r[4])
                status = "RESOLVED_AD" if h and h["level"] == "AD" else \
                         "RESOLVED_ADSET" if h else "UNRESOLVED"

                batch.append((
                    r[0], r[1], r[2], r[3], r[4],
                    h.get("fb_ad_id") if h else None,
                    h.get("fb_ad_name") if h else None,
                    h.get("fb_adset_id") if h else None,
                    h.get("fb_adset_name") if h else None,
                    h.get("fb_campaign_id") if h else None,
                    h.get("fb_campaign_name") if h else None,
                    h.get("fb_campaign_objective") if h else None,
                    h.get("resolved_with_token") if h else None,
                    status,
                    datetime.now(timezone.utc)
                ))

            execute_batch(cur, insert_sql, batch)
            conn.commit()
            print(f"✅ Inserted {len(batch)} new rows")

        # -------------------------------
        # PHASE 2: RECOVER UNRESOLVED
        # -------------------------------
        print("🔁 Retrying unresolved rows...")

        cur.execute("""
        SELECT row_id, utm_term
        FROM shopify_facebook_attribution
        WHERE resolution_status = 'UNRESOLVED'
          AND utm_term ~ '^[0-9]{15,}$'
        LIMIT 500
        """)

        rows = cur.fetchall()
        if rows:
            ids = {r[1] for r in rows}
            with ThreadPoolExecutor(MAX_WORKERS) as ex:
                list(as_completed([ex.submit(resolve_meta_object, i) for i in ids]))

            update_sql = """
            UPDATE shopify_facebook_attribution SET
              fb_ad_id=%s, fb_ad_name=%s,
              fb_adset_id=%s, fb_adset_name=%s,
              fb_campaign_id=%s, fb_campaign_name=%s,
              fb_campaign_objective=%s,
              resolved_with_token=%s,
              resolution_status=%s,
              resolved_at=%s
            WHERE row_id=%s
            """

            batch = []
            for row_id, term in rows:
                h = meta_cache.get(term)
                if h:
                    batch.append((
                        h["fb_ad_id"], h["fb_ad_name"],
                        h["fb_adset_id"], h["fb_adset_name"],
                        h["fb_campaign_id"], h["fb_campaign_name"],
                        h["fb_campaign_objective"],
                        h["resolved_with_token"],
                        "RESOLVED_AD" if h["level"] == "AD" else "RESOLVED_ADSET",
                        datetime.now(timezone.utc),
                        row_id
                    ))

            if batch:
                execute_batch(cur, update_sql, batch)
                conn.commit()
                print(f"♻️ Recovered {len(batch)} rows")

    finally:
        try:
            cur.execute("SELECT pg_advisory_unlock(%s);", (ADVISORY_LOCK_ID,))
            conn.commit()
        except Exception:
            pass
        cur.close()
        conn.close()

# =====================================================
# ENTRYPOINT
# =====================================================

if __name__ == "__main__":
    threading.Thread(target=start_health_server, daemon=True).start()
    print("🚀 Meta attribution worker running (backfill + recovery)")

    while True:
        try:
            run_cycle()
        except Exception as e:
            print("❌ Worker error:", e)

        print(f"😴 Sleeping {SLEEP_SECONDS}s")
        time.sleep(SLEEP_SECONDS)
