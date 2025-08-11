import os
import io
import base64
import tempfile
import requests
import boto3
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any
from openai import OpenAI
from ..config import AppConfig
from .prompt_manager import PromptManager
from ..db.redshift import redshift_conn
from .approval_store import insert_generation_rows

CFG = AppConfig()
S3 = boto3.client('s3', aws_access_key_id=CFG.aws.access_key_id, aws_secret_access_key=CFG.aws.secret_access_key)
OPENAI = OpenAI(api_key=CFG.openai.api_key)
PROMPTS = PromptManager(prompts_folder='prompts')


def query_deals(category: str, limit: int = 10) -> pd.DataFrame:
    sql = f"""
    WITH deal_revenue AS (
        SELECT t.deal_id, SUM(t.net) AS total_revenue
        FROM real.transactions t
        JOIN real.deal_voucher dv ON t.deal_id = dv.id
        WHERE t.order_date > TRUNC(SYSDATE - 8)
          AND dv.currency = 'GBP'
          AND t.brand_id = 1
          AND t.domain = 'WOWCHER'
        GROUP BY t.deal_id
    )
    SELECT CAST(dv.id AS INTEGER) AS id,
           dv.email_subject AS email_subject,
           dvc.name AS category_name,
           dvc.canonical_path_type as vertical,
           dvsc.name AS sub_category_name,
           CAST(COALESCE(dr.total_revenue, 0) AS DECIMAL(10,2)) AS revenue_last_7_days,
           CAST(rank() OVER (ORDER BY COALESCE(dr.total_revenue, 0) DESC) AS INTEGER) AS revenue_rank,
           dvi.id AS image_id_pos_0,
           'https://static.wowcher.co.uk/images/deal/' || dvi.deal_voucher_id || '/' || dvi.id || '.' || dvi.extension AS image_url_pos_0,
           dvi.extension
    FROM real.deal_voucher dv
    LEFT JOIN deal_revenue dr ON dr.deal_id = dv.id
    LEFT JOIN real.deal_voucher_image dvi ON dvi.deal_voucher_id = dv.id AND dvi.position = 0
    LEFT JOIN real.deal_voucher_category dvc ON dvc.id = dv.category_id
    LEFT JOIN real.deal_voucher_sub_category dvsc ON dvsc.id = dv.sub_category_id
    WHERE trunc(dv.closing_date) >= trunc(sysdate) + 21
      AND dvi.id IS NOT NULL
      AND dv.currency = 'GBP'
      AND dvc.name = %s
    ORDER BY COALESCE(dr.total_revenue, 0) DESC
    LIMIT %s
    """
    with redshift_conn() as conn:
        df = pd.read_sql(sql, conn, params=(category, limit))
        print(f"[generation] query_deals: category='{category}', limit={limit}, rows={len(df)}")
        return df


def download(url: str) -> bytes:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.content


def generate_one(deal: Dict[str, Any]) -> Dict[str, Any]:
    prompt, prompt_source = PROMPTS.get_prompt(
        vertical=deal.get('vertical'),
        category_name=deal.get('category_name'),
        sub_category_name=deal.get('sub_category_name'),
        email_subject=deal.get('email_subject'),
        formatted_highlights="",
    )

    # prepare original images (use up to 8 first deal images)
    image_urls = [deal.get('image_url_pos_0')]
    image_files = []
    with tempfile.TemporaryDirectory() as td:
        for idx, u in enumerate(image_urls):
            try:
                p = os.path.join(td, f"img_{idx}.png")
                with open(p, 'wb') as f:
                    f.write(download(u))
                image_files.append(open(p, 'rb'))
            except Exception:
                continue

        # OpenAI image edit
        try:
            print(f"[generation] OpenAI edit start: deal={deal.get('id')}")
            result = OPENAI.images.edit(
                model="gpt-image-1",
                image=image_files,
                prompt=prompt,
                size="1536x1024",
                quality="high",
                background="auto",
                n=1
            )
        except Exception as e:
            raise RuntimeError(f"OpenAI image edit failed for deal {deal.get('id')}: {e}")
        for f in image_files:
            try:
                f.close()
            except Exception:
                pass

        image_base64 = result.data[0].b64_json
        image_bytes = base64.b64decode(image_base64)

    # Upload to S3
    key = f"images/deal/{deal['id']}/{deal['image_id_pos_0']}_variant.jpg"
    S3.put_object(
        Body=image_bytes,
        Bucket=CFG.aws.bucket_name,
        Key=key,
        ContentType='image/jpeg',
        CacheControl='no-cache, no-store, must-revalidate'
    )
    s3_url = f"https://{CFG.aws.bucket_name}/{key}"
    print(f"[generation] Uploaded variant to {s3_url}")

    return {
        **deal,
        's3_url': s3_url,
        'prompt': prompt,
        'prompt_source': prompt_source,
        'token_info': {},
    }


def run_batch(category: str, limit: int = 5) -> int:
    df = query_deals(category=category, limit=limit)
    if df is None or df.empty:
        raise ValueError(f"No deals found for category '{category}'.")
    deals = df.to_dict(orient='records')
    results: List[Dict[str, Any]] = []
    print(f"[generation] Starting batch: {len(deals)} deals")
    # Reduce parallelism to avoid rate limits/timeouts
    with ThreadPoolExecutor(max_workers=25) as ex:
        futures = [ex.submit(generate_one, d) for d in deals]
        for fut in as_completed(futures):
            try:
                res = fut.result()
                results.append(res)
                print(f"[generation] Generated deal={res.get('id')}")
            except Exception as e:
                # Skip failed item; continue with others
                print(f"[generation] Item failed: {e}")
    if results:
        insert_generation_rows(results)
        print(f"[generation] Inserted {len(results)} rows into temp.image_to_approve")
    return len(results)
