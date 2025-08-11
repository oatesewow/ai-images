from ..db.redshift import redshift_conn
from typing import List, Dict, Any

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS temp.image_to_approve (
  id BIGINT IDENTITY(1,1),
  deal_voucher_id BIGINT NOT NULL,
  image_id_pos_0 BIGINT,
  original_url VARCHAR(1024),
  variant_s3_url VARCHAR(1024),
  prompt_source VARCHAR(256),
  prompt VARCHAR(65535),
  token_info VARCHAR(65535),
  vertical VARCHAR(64),
  category_name VARCHAR(256),
  sub_category_name VARCHAR(256),
  status VARCHAR(32) DEFAULT 'pending',
  reviewer VARCHAR(128),
  review_notes VARCHAR(2048),
  created_ts TIMESTAMP DEFAULT GETDATE(),
  reviewed_ts TIMESTAMP
);
"""

MIGRATIONS = [
    "ALTER TABLE temp.image_to_approve ALTER COLUMN original_url TYPE VARCHAR(1024)",
    "ALTER TABLE temp.image_to_approve ALTER COLUMN variant_s3_url TYPE VARCHAR(1024)",
    "ALTER TABLE temp.image_to_approve ALTER COLUMN prompt_source TYPE VARCHAR(256)",
    "ALTER TABLE temp.image_to_approve ALTER COLUMN prompt TYPE VARCHAR(65535)",
    "ALTER TABLE temp.image_to_approve ALTER COLUMN token_info TYPE VARCHAR(65535)",
    "ALTER TABLE temp.image_to_approve ALTER COLUMN category_name TYPE VARCHAR(256)",
    "ALTER TABLE temp.image_to_approve ALTER COLUMN sub_category_name TYPE VARCHAR(256)",
    "ALTER TABLE temp.image_to_approve ALTER COLUMN review_notes TYPE VARCHAR(2048)"
]

def ensure_schema() -> None:
    with redshift_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
            # Best-effort widen columns to prevent truncation errors
            for stmt in MIGRATIONS:
                try:
                    cur.execute(stmt)
                except Exception:
                    pass
            conn.commit()


def insert_generation_rows(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    def _s(val, maxlen: int):
        if val is None:
            return None
        text = str(val)
        return text[:maxlen]
    sql = """
    INSERT INTO temp.image_to_approve (
      deal_voucher_id, image_id_pos_0, original_url, variant_s3_url,
      prompt_source, prompt, token_info, vertical, category_name, sub_category_name
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    values = []
    for r in rows:
        values.append(
            (
                r.get("id"),
                r.get("image_id_pos_0"),
                _s(r.get("image_url_pos_0"), 1024),
                _s(r.get("s3_url"), 1024),
                _s(r.get("prompt_source"), 256),
                _s(r.get("prompt"), 65535),
                _s(r.get("token_info"), 65535),
                _s(r.get("vertical"), 64),
                _s(r.get("category_name"), 256),
                _s(r.get("sub_category_name"), 256),
            )
        )
    with redshift_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.executemany(sql, values)
                conn.commit()
            except Exception as e:
                conn.rollback()
                # Fallback to per-row to locate offending data and still insert others
                inserted = 0
                for v in values:
                    try:
                        cur.execute(sql, v)
                        inserted += 1
                        conn.commit()
                    except Exception as inner:
                        conn.rollback()
                        print(f"[approval_store] row insert failed: {inner} value={v}")
                if inserted == 0:
                    raise e


def list_pending(limit: int = 100, offset: int = 0):
    sql = """
    SELECT id, deal_voucher_id, image_id_pos_0, original_url, variant_s3_url,
           prompt_source, prompt, vertical, category_name, sub_category_name,
           created_ts
    FROM temp.image_to_approve
    WHERE status = 'pending'
    ORDER BY created_ts DESC
    LIMIT %s OFFSET %s
    """
    with redshift_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (limit, offset))
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


def update_review(item_id: int, status: str, reviewer: str, notes: str) -> None:
    sql = """
    UPDATE temp.image_to_approve
    SET status = %s, reviewer = %s, review_notes = %s, reviewed_ts = GETDATE()
    WHERE id = %s
    """
    with redshift_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (status, reviewer, notes, item_id))
            conn.commit()
