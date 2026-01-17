import os
import time
import requests
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

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
BATCH_SIZE = 1000
ADVISORY_LOCK_ID = 777001

# =====================================================
# GLOBAL CACHES
# =====================================================

meta_cache = {}
failed_ids = set()

# =====================================================
# META RESOLUTION (AD → ADSET FALLBACK)
# =====================================================

def resolve_meta_object(object_id):
    if object_id in meta_cache:
        return meta_cache[object_id]
    if object_id in failed_ids:
        return None

    for token_name, token in TOKENS:
        try:
            # ---------- TRY AS AD ----------
            ad_r = requests.get(
                f"{GRAPH}/{object_id}",
                params={
                    "access_token": token,
                    "fields": "name,adset_id,campaign_id"
                },
                timeout=10
            )

            if ad_r.status_code == 429:
                time.sleep(1)
                continue

            if ad_r.status_code == 200:
                ad = ad_r.json()
                adset_id = ad.get("adset_id")
                campaign_id = ad.get("campaign_id")

                if adset_id and campaign_id:
                    adset_r = requests.get(
                        f"{GRAPH}/{adset_id}",
                        params={"access_token": token, "fields": "name"},
                        timeout=10
                    )
                    camp_r = requests.get(
                        f"{GRAPH}/{campaign_id}",
                        params={"access_token": token, "fields": "name,objective"},
                        timeout=10
                    )

                    if adset_r.status_code == 200 and camp_r.status_code == 200:
                        result = {
                            "level": "AD",
                            "fb_ad_id": object_id,
                            "fb_ad_name": ad.get("name"),
                            "fb_adset_id": adset_id,
                            "fb_adset_name": adset_r.json().get("name"),
                            "fb_campaign_id": campaign_id,
                            "fb_campaign_name": camp_r.json().get("name"),
                            "fb_campaign_objective": camp_r.json().get("objective"),
                            "resolved_with_token": token_name,
                        }
                        meta_cache[object_id] = result
                        return result

            # ---------- TRY AS ADSET ----------
            adset_r = requests.get(
                f"{GRAPH}/{object_id}",
                params={
                    "access_token": token,
                    "fields": "name,campaign_id"
                },
                timeout=10
            )

            if adset_r.status_code == 429:
                time.sleep(1)
                continue

            if adset_r.status_code == 200:
                adset = adset_r.json()
                campaign_id = adset.get("campaign_id")

                if campaign_id:
                    camp_r = requests.get(
                        f"{GRAPH}/{campaign_id}",
                        params={"access_token": token, "fields": "name,objective"},
                        timeout=10
                    )

                    if camp_r.status_code == 200:
                        result = {
                            "level": "ADSET",
                            "fb_ad_id": None,
                            "fb_ad_name": None,
                            "fb_adset_id": object_id,
                            "fb_adset_name": adset.get("name"),
                            "fb_campaign_id": campaign_id,
                            "fb_campaign_name": camp_r.json().get("name"),
                            "fb_campaign_objective": camp_r.json().get("objective"),
                            "resolved_with_token": token_name,
                        }
                        meta_cache[object_id] = result
                        return result

        except Exception:
            continue

    failed_ids.add(object_id)
    return None

# =====================================================
# MAIN
# =====================================================

def main():
    print("🔌 Connecting to database...")
    conn = psycopg2.connect(POSTGRES_URI)
    cur = conn.cursor()

    try:
        cur.execute("SET statement_timeout = 0;")
        cur.execute("SELECT pg_try_advisory_lock(%s);", (ADVISORY_LOCK_ID,))
        locked = cur.fetchone()[0]

        if not locked:
            print("⛔ Another attribution job is already running")
            return

        conn.commit()
        print("✅ Connected & lock acquired")

        # -------------------------------------------------
        # LOAD UNRESOLVED ROWS
        # -------------------------------------------------

        print("📦 Loading unresolved Meta rows...")

        cur.execute("""
        SELECT
          row_id,
          utm_source,
          utm_campaign,
          utm_content,
          utm_term
        FROM shopify_facebook_attribution
        WHERE resolution_status = 'UNRESOLVED'
          AND LOWER(utm_source) IN ('facebook','fb','ig','instagram')
          AND utm_term ~ '^[0-9]{15,}$'
        """)

        rows = cur.fetchall()
        total = len(rows)
        print(f"🚀 Reprocessing {total} unresolved rows")

        if total == 0:
            print("✅ Nothing to process")
            return

        # -------------------------------------------------
        # PARALLEL META RESOLUTION
        # -------------------------------------------------

        unique_ids = sorted({r[4] for r in rows})
        print(f"⚡ Resolving {len(unique_ids)} unique Meta IDs...")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(resolve_meta_object, oid): oid for oid in unique_ids}
            for idx, _ in enumerate(as_completed(futures), start=1):
                if idx % 500 == 0:
                    print(f"   🔄 {idx}/{len(unique_ids)} resolved")

        print("✅ Meta resolution complete")

        # -------------------------------------------------
        # UPDATE ROWS
        # -------------------------------------------------

        update_sql = """
        UPDATE shopify_facebook_attribution SET
          fb_ad_id = %s,
          fb_ad_name = %s,
          fb_adset_id = %s,
          fb_adset_name = %s,
          fb_campaign_id = %s,
          fb_campaign_name = %s,
          fb_campaign_objective = %s,
          resolved_with_token = %s,
          resolution_status = %s,
          resolution_reason = NULL,
          resolved_at = %s
        WHERE row_id = %s
        """

        batch = []
        processed = 0

        for row_id, src, camp, content, term in rows:
            h = meta_cache.get(term)

            if h:
                status = "RESOLVED_AD" if h["level"] == "AD" else "RESOLVED_ADSET"
                batch.append((
                    h["fb_ad_id"],
                    h["fb_ad_name"],
                    h["fb_adset_id"],
                    h["fb_adset_name"],
                    h["fb_campaign_id"],
                    h["fb_campaign_name"],
                    h["fb_campaign_objective"],
                    h["resolved_with_token"],
                    status,
                    datetime.now(timezone.utc),
                    row_id
                ))

            if len(batch) >= BATCH_SIZE:
                execute_batch(cur, update_sql, batch)
                conn.commit()
                processed += len(batch)
                print(f"💾 {processed}/{total} updated")
                batch.clear()

        if batch:
            execute_batch(cur, update_sql, batch)
            conn.commit()
            processed += len(batch)

        print(f"🎉 Done: {processed}/{total} rows recovered")

    finally:
        try:
            cur.execute("SELECT pg_advisory_unlock(%s);", (ADVISORY_LOCK_ID,))
            conn.commit()
        except Exception:
            pass

        cur.close()
        conn.close()
        print("🎯 FACEBOOK / IG ATTRIBUTION RECOVERY COMPLETE")

# =====================================================
# ENTRY POINT
# =====================================================

if __name__ == "__main__":
    main()
