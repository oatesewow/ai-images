import pandas as pd
import boto3
import requests
from dotenv import load_dotenv
import os
import oracledb
import psycopg2
from datetime import datetime
import time
from urllib.parse import urlparse
import tempfile
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from PIL import Image, ImageOps
import io

# Load environment variables
load_dotenv()

# Configuration settings
AWS_CONFIG = {
    'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
    'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY')
}

S3_CONFIG = {
    'bucket_name': os.getenv('S3_BUCKET_NAME', 'static.wowcher.co.uk')
}

REDSHIFT_CONFIG = {
    'host': os.getenv('REDSHIFT_HOST'),
    'port': os.getenv('REDSHIFT_PORT'),
    'dbname': os.getenv('REDSHIFT_DBNAME'),
    'user': os.getenv('REDSHIFT_USER'),
    'password': os.getenv('REDSHIFT_PASSWORD')
}

# Oracle configuration
ORACLE_CONFIG = {
    'user': os.getenv("ORACLE_USER"),
    'password': os.getenv("ORACLE_PASSWORD"),
    'dsn': os.getenv("ORACLE_DSN")
}

# Target sizes for image variants
TARGET_SIZES = {
    "": (777, 520),
    "-cashback-promo": (126, 90),
    "-email": (640, 428),
    "-thumb": (105, 70),
    "-promo": (172, 115),
    "-user": (50, 50),
    "-deal-bonus": (278, 182),
    "-iphone-medium": (151, 106),
    "-iphone-small": (80, 53),
    "-iphone-promo": (640, 428),
    "-iphone-thumb": (210, 140),
    "-travel-main": (777, 520),
    "-2-per-row": (470, 315),
    "-3-per-row": (310, 210),
}

def uncrop_image(image, target_aspect_ratio):
    """
    Adjust image to target aspect ratio by adding padding if needed
    """
    original_width, original_height = image.size
    original_aspect = original_width / original_height
    
    if abs(original_aspect - target_aspect_ratio) < 1e-6:
        return image
    
    if original_aspect > target_aspect_ratio:
        # Image is too wide, add padding top/bottom
        new_height = int(original_width / target_aspect_ratio)
        new_image = Image.new('RGB', (original_width, new_height), (255, 255, 255))
        paste_y = (new_height - original_height) // 2
        new_image.paste(image, (0, paste_y))
    else:
        # Image is too tall, add padding left/right
        new_width = int(original_height * target_aspect_ratio)
        new_image = Image.new('RGB', (new_width, original_height), (255, 255, 255))
        paste_x = (new_width - original_width) // 2
        new_image.paste(image, (paste_x, 0))
    
    return new_image

def generate_variants(image_id, original_image, temp_dir):
    """
    Generate all required image variants and return their file paths
    """
    variant_files = {}
    
    # Generate main variant first
    first_variant_size = TARGET_SIZES[""]
    first_variant_aspect = first_variant_size[0] / first_variant_size[1]
    base_variant = uncrop_image(original_image, first_variant_aspect)
    base_variant = base_variant.resize(first_variant_size, Image.LANCZOS)
    
    # Save main variant
    main_path = os.path.join(temp_dir, f"{image_id}.jpg")
    base_variant.save(main_path, "JPEG")
    variant_files[""] = main_path
    
    # Generate all other variants
    for suffix, (w, h) in TARGET_SIZES.items():
        if suffix == "":
            continue
            
        filename = f"{image_id}{suffix}.jpg"
        variant_path = os.path.join(temp_dir, filename)
        
        if suffix == "-user":
            # Special handling for user avatar (square)
            orig_aspect = original_image.width / original_image.height
            if abs(orig_aspect - 1.0) < 1e-6:
                resized = original_image.resize((w, h), Image.LANCZOS)
            else:
                square_variant = uncrop_image(original_image, 1.0)
                resized = square_variant.resize((w, h), Image.LANCZOS)
        else:
            resized = base_variant.resize((w, h), Image.LANCZOS)
        
        resized.save(variant_path, "JPEG")
        variant_files[suffix] = variant_path
    
    return variant_files

def get_new_oracle_image_id():
    """
    Get a new image ID from Oracle sequence
    Tries deal_voucher_image_seq first, then falls back to product_image_seq
    """
    connection = oracledb.connect(**ORACLE_CONFIG)
    cursor = connection.cursor()
    
    # Try the proper sequence for DEAL_VOUCHER_IMAGE table first
    try:
        cursor.execute("SELECT deal_voucher_image_seq.NEXTVAL FROM dual")
    except Exception as e:
        # Fall back to product_image_seq if deal_voucher_image_seq doesn't exist
        try:
            cursor.execute("SELECT product_image_seq.NEXTVAL FROM dual")
        except Exception as e2:
            cursor.close()
            connection.close()
            raise Exception(f"Could not find a suitable sequence for images. Tried deal_voucher_image_seq: {e}, product_image_seq: {e2}")
    
    new_image_id = cursor.fetchone()[0]
    connection.commit()
    cursor.close()
    connection.close()
    return new_image_id

def get_deal_id_from_image_id(image_id):
    """
    Get the deal_voucher_id for a given image_id from Oracle
    """
    connection = oracledb.connect(**ORACLE_CONFIG)
    cursor = connection.cursor()
    
    cursor.execute("""
        SELECT DEAL_VOUCHER_ID 
        FROM DEAL_VOUCHER_IMAGE 
        WHERE ID = :image_id
    """, {"image_id": image_id})
    
    result = cursor.fetchone()
    cursor.close()
    connection.close()
    
    if result:
        return result[0]
    else:
        raise ValueError(f"No deal found for image_id {image_id}")

def download_variant_image(image_id, temp_dir):
    """
    Download the variant image (image_id + "00000") from S3 and return both file path and PIL Image
    """
    try:
        # Connect to S3
        s3_client = boto3.client('s3', **AWS_CONFIG)
        bucket_name = S3_CONFIG['bucket_name']
        
        # Get deal_id to construct the S3 path
        deal_id = get_deal_id_from_image_id(image_id)
        
        # Construct the variant image key (original image_id + 00000)
        variant_image_id = f"{image_id}00000"
        variant_key = f"images/deal/{deal_id}/{variant_image_id}.jpg"
        
        # Download the variant image to a temporary file
        temp_file_path = os.path.join(temp_dir, f"{variant_image_id}.jpg")
        s3_client.download_file(bucket_name, variant_key, temp_file_path)
        
        # Also load as PIL Image for processing
        original_image = Image.open(temp_file_path)
        
        return temp_file_path, deal_id, original_image
        
    except Exception as e:
        print(f"Error downloading variant image for {image_id}: {str(e)}")
        raise

def upload_variants_to_s3(variant_files, deal_id, new_image_id):
    """
    Upload all image variants to S3 with the new image ID
    """
    try:
        s3_client = boto3.client('s3', **AWS_CONFIG)
        bucket_name = S3_CONFIG['bucket_name']
        uploaded_urls = {}
        
        for suffix, local_file_path in variant_files.items():
            # Create new S3 key with new image ID and suffix
            new_key = f"images/deal/{deal_id}/{new_image_id}{suffix}.jpg"
            
            # Upload the file
            with open(local_file_path, 'rb') as file_data:
                s3_client.upload_fileobj(
                    file_data,
                    bucket_name,
                    new_key,
                    ExtraArgs={
                        'ContentType': 'image/jpeg',
                        'CacheControl': 'no-cache'
                    }
                )
            
            final_url = f"https://{bucket_name}/{new_key}"
            uploaded_urls[suffix] = final_url
            #print(f"✅ Uploaded variant: {new_image_id}{suffix}.jpg")
        
        return uploaded_urls
        
    except Exception as e:
        print(f"Error uploading variants to S3: {str(e)}")
        raise

def get_current_image_positions(deal_id):
    """
    Get all current images for a deal with their positions
    """
    connection = oracledb.connect(**ORACLE_CONFIG)
    cursor = connection.cursor()
    
    cursor.execute("""
        SELECT ID, POSITION, FILE_NAME, RESOURCE_PATH, CAPTION, ALT_TAG, EXTENSION, HAS_IPHONE_IMG
        FROM DEAL_VOUCHER_IMAGE 
        WHERE DEAL_VOUCHER_ID = :deal_id 
        AND STATUS_ID = 1
        ORDER BY POSITION
    """, {"deal_id": deal_id})
    
    results = cursor.fetchall()
    cursor.close()
    connection.close()
    
    return results

def update_image_positions_and_insert_new(original_image_id, new_image_id, deal_id, new_file_name):
    """
    Update image positions and insert new image:
    1. New image goes to position 0
    2. Image that was in position 0 (being replaced) moves to position 2
    3. Image in position 1 stays the same
    4. All other images shift down one position
    """
    connection = None
    cursor = None
    
    try:
        connection = oracledb.connect(**ORACLE_CONFIG)
        cursor = connection.cursor()
        
        # Get current images
        current_images = get_current_image_positions(deal_id)
        
        if not current_images:
            raise ValueError(f"No images found for deal {deal_id}")
        
        # Find the image being replaced (original_image_id)
        replaced_image = None
        for img in current_images:
            if img[0] == original_image_id:
                replaced_image = img
                break
        
        if not replaced_image:
            raise ValueError(f"Original image {original_image_id} not found in deal {deal_id}")
        
        original_position = replaced_image[1]
        
        # Step 1: Update all positions to make room for new arrangement
        # First, move everything to temporary high positions to avoid conflicts
        temp_position_offset = 1000
        for img in current_images:
            cursor.execute("""
                UPDATE DEAL_VOUCHER_IMAGE 
                SET POSITION = :temp_pos
                WHERE ID = :image_id AND DEAL_VOUCHER_ID = :deal_id
            """, {
                "temp_pos": img[1] + temp_position_offset,
                "image_id": img[0],
                "deal_id": deal_id
            })
        
        # Step 2: Apply the new position logic
        for img in current_images:
            current_id = img[0]
            current_pos = img[1]
            
            if current_id == original_image_id:
                # The replaced image moves to position 2
                new_pos = 2
            elif current_pos == 1:
                # Image in position 1 stays at position 1
                new_pos = 1
            else:
                # All other images shift down one position
                if current_pos == 0:
                    # This shouldn't happen if original_image_id was at position 0
                    # But if there are multiple images at position 0, handle it
                    new_pos = 3
                else:
                    new_pos = current_pos + 1
            
            cursor.execute("""
                UPDATE DEAL_VOUCHER_IMAGE 
                SET POSITION = :new_pos
                WHERE ID = :image_id AND DEAL_VOUCHER_ID = :deal_id
            """, {
                "new_pos": new_pos,
                "image_id": current_id,
                "deal_id": deal_id
            })
        
        # Step 3: Insert the new image at position 0
        # Use details from the replaced image as template
        cursor.execute("""
            INSERT INTO DEAL_VOUCHER_IMAGE (
                ID, DEAL_VOUCHER_ID, RESOURCE_PATH, STATUS_ID,
                FILE_NAME, CAPTION, POSITION, ALT_TAG,
                EXTENSION, HAS_IPHONE_IMG, CREATED_BY_USER_ID, CREATED_DATE
            ) VALUES (
                :new_image_id, :deal_id, :resource_path, 1,
                :file_name, :caption, 0, :alt_tag,
                :extension, :has_iphone_img, 18282217, CURRENT_TIMESTAMP
            )
        """, {
            "new_image_id": new_image_id,
            "deal_id": deal_id,
            "resource_path": replaced_image[3],  # Use same resource path
            "file_name": new_file_name,
            "caption": f"Variant image {new_image_id}",
            "alt_tag": f"Variant image {new_image_id}",
            "extension": replaced_image[6] or 'jpg',
            "has_iphone_img": replaced_image[7] or 0
        })
        
        connection.commit()
        
        return True
        
    except Exception as e:
        print(f"Error updating image positions: {str(e)}")
        if connection:
            connection.rollback()
        return False
        
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def process_image_replacement(image_id):
    """
    Main function to process image replacement:
    1. Download variant image (image_id + "00000")
    2. Get new Oracle image ID
    3. Generate all image variants
    4. Upload all variants to S3 with new ID
    5. Update positions in Oracle (new->0, old->2, others shift)
    """
    temp_dir = None
    
    try:
        # Create temporary directory for downloads
        temp_dir = tempfile.mkdtemp()
        
        # Step 1: Download the variant image
        temp_file_path, deal_id, original_image = download_variant_image(image_id, temp_dir)
        
        # Step 2: Get new Oracle image ID
        new_image_id = get_new_oracle_image_id()
        
        # Step 3: Generate all image variants
        print(f"Generating variants for image {new_image_id}...")
        variant_files = generate_variants(new_image_id, original_image, temp_dir)
        
        # Step 4: Upload all variants to S3 with new image ID
        uploaded_urls = upload_variants_to_s3(variant_files, deal_id, new_image_id)
        
        # Step 5: Update Oracle records and positions
        new_file_name = f"{new_image_id}.jpg"
        success = update_image_positions_and_insert_new(
            image_id, new_image_id, deal_id, new_file_name
        )
        
        if success:
            return {
                'success': True,
                'original_image_id': image_id,
                'new_image_id': new_image_id,
                'deal_id': deal_id,
                'uploaded_urls': uploaded_urls,
                'variants_count': len(variant_files)
            }
        else:
            return {
                'success': False,
                'error': 'Failed to update Oracle records'
            }
        
    except Exception as e:
        print(f"Error in image replacement process: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }
        
    finally:
        # Clean up temporary directory
        if temp_dir and os.path.exists(temp_dir):
            import shutil
            shutil.rmtree(temp_dir)

def update_redshift_status_single(original_image_id, new_variant_image_id=None):
    """
    Update Redshift temp.opt_image_variants table for a single image:
    - Set status = 3 
    - Set exit_test_ts = current timestamp
    - Set variant_image_id = new Oracle ID (if provided)
    - Where original_image_id matches and status = 1
    """
    connection = None
    cursor = None
    
    try:
        connection = psycopg2.connect(**REDSHIFT_CONFIG)
        cursor = connection.cursor()
        
        # Build update query based on whether new_variant_image_id is provided
        if new_variant_image_id:
            update_query = """
                UPDATE temp.opt_image_variants 
                SET status = 3, exit_test_ts = CURRENT_TIMESTAMP, variant_image_id = %s
                WHERE original_image_id = %s AND status = 1
            """
            cursor.execute(update_query, (new_variant_image_id, original_image_id))
        else:
            update_query = """
                UPDATE temp.opt_image_variants 
                SET status = 3, exit_test_ts = CURRENT_TIMESTAMP
                WHERE original_image_id = %s AND status = 1
            """
            cursor.execute(update_query, (original_image_id,))
        
        rows_updated = cursor.rowcount
        connection.commit()
        
        return rows_updated > 0
            
    except Exception as e:
        print(f"Error updating Redshift for image {original_image_id}: {str(e)}")
        if connection:
            connection.rollback()
        return False
        
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()



def update_redshift_status_batch(image_mapping):
    """
    Ultra-fast Redshift batch update using temporary table approach
    
    Args:
        image_mapping: dict {original_id: new_variant_id}
    """
    connection = None
    cursor = None
    
    try:
        batch_size = len(image_mapping)
        print(f"Updating Redshift status and variant IDs for {batch_size} images using temp table...")
        
        connection = psycopg2.connect(**REDSHIFT_CONFIG)
        cursor = connection.cursor()
        
        # Create temporary table
        cursor.execute("""
            CREATE TEMP TABLE temp_image_updates (
                original_image_id BIGINT,
                new_variant_image_id BIGINT
            )
        """)
        
        # Insert data into temp table using VALUES
        values_list = []
        for original_id, new_variant_id in image_mapping.items():
            values_list.append(f"({original_id}, {new_variant_id})")
        
        values_clause = ",".join(values_list)
        insert_query = f"""
            INSERT INTO temp_image_updates (original_image_id, new_variant_image_id)
            VALUES {values_clause}
        """
        
        cursor.execute(insert_query)
        
        # Single UPDATE with JOIN
        update_query = """
            UPDATE temp.opt_image_variants 
            SET 
                status = 3,
                exit_test_ts = CURRENT_TIMESTAMP,
                variant_image_id = temp_updates.new_variant_image_id
            FROM temp_image_updates temp_updates
            WHERE temp.opt_image_variants.original_image_id = temp_updates.original_image_id 
            AND temp.opt_image_variants.status = 1
        """
        
        cursor.execute(update_query)
        rows_updated = cursor.rowcount
        
        # Clean up temp table
        cursor.execute("DROP TABLE temp_image_updates")
        
        connection.commit()
        
        if rows_updated > 0:
            print(f"✅ Updated {rows_updated} Redshift record(s) in single query")
            return True
        else:
            print(f"⚠️ No Redshift records found to update")
            return False
            
    except Exception as e:
        print(f"Error in Redshift batch update: {str(e)}")
        if connection:
            connection.rollback()
        return False
        
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def process_image_replacement_with_redshift(image_id, update_redshift=True):
    """
    Complete workflow including Redshift update:
    1. Process image replacement
    2. Update Redshift status and variant_image_id if successful
    """
    try:
        # Step 1: Process the image replacement
        result = process_image_replacement(image_id)
        
        # Step 2: Update Redshift if processing was successful and requested
        if result['success'] and update_redshift:
            new_oracle_id = result.get('new_image_id')
            redshift_success = update_redshift_status_single(image_id, new_oracle_id)
            result['redshift_updated'] = redshift_success
        else:
            result['redshift_updated'] = False
            
        return result
        
    except Exception as e:
        print(f"Error in complete workflow for image {image_id}: {str(e)}")
        return {
            'success': False,
            'error': str(e),
            'redshift_updated': False
        }

def process_batch_with_workers(image_ids, max_workers=25, update_redshift=True):
    """
    Process multiple image replacements with multi-threading
    
    Args:
        image_ids: List of image IDs to process
        max_workers: Number of parallel workers (default: 25)
        update_redshift: Whether to update Redshift at the end (default: True)
    
    Returns:
        dict with results and summary
    """
    print(f"Processing {len(image_ids)} images with {max_workers} workers...")
    print("=" * 60)
    
    results = []
    successful_mapping = {}
    failed_images = []
    
    # Process images in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all jobs
        future_to_image = {
            executor.submit(process_image_replacement, image_id): image_id 
            for image_id in image_ids
        }
        
        # Process completed jobs with progress bar
        with tqdm(total=len(image_ids), desc="Processing Images") as pbar:
            for future in as_completed(future_to_image):
                image_id = future_to_image[future]
                
                try:
                    result = future.result()
                    results.append({
                        'image_id': image_id,
                        'result': result
                    })
                    
                    if result['success']:
                        successful_mapping[image_id] = result['new_image_id']
                        pbar.set_postfix({"✅": len(successful_mapping), "❌": len(failed_images)})
                    else:
                        failed_images.append({
                            'image_id': image_id,
                            'error': result.get('error', 'Unknown error')
                        })
                        pbar.set_postfix({"✅": len(successful_mapping), "❌": len(failed_images)})
                        
                except Exception as e:
                    failed_images.append({
                        'image_id': image_id,
                        'error': str(e)
                    })
                    pbar.set_postfix({"✅": len(successful_mapping), "❌": len(failed_images)})
                
                pbar.update(1)
    
    # Update Redshift for all successful replacements
    redshift_success = False
    if successful_mapping and update_redshift:
        print(f"\nUpdating Redshift for {len(successful_mapping)} successful replacements...")
        redshift_success = update_redshift_status_batch(successful_mapping)
    
    # Print summary
    print("\n" + "=" * 60)
    print("BATCH PROCESSING SUMMARY")
    print("=" * 60)
    print(f"Total processed: {len(image_ids)}")
    print(f"Successful: {len(successful_mapping)}")
    print(f"Failed: {len(failed_images)}")
    
    if redshift_success:
        print("✅ Redshift batch update completed")
    elif update_redshift and successful_mapping:
        print("⚠️ Redshift batch update failed")
    
    if successful_mapping:
        print(f"\nSuccessful replacements:")
        for original_id, new_id in successful_mapping.items():
            print(f"  {original_id} → {new_id}")
    
    if failed_images:
        print(f"\nFailed replacements:")
        for failure in failed_images:
            print(f"  {failure['image_id']}: {failure['error']}")
    
    return {
        'total_processed': len(image_ids),
        'successful_count': len(successful_mapping),
        'failed_count': len(failed_images),
        'successful_mapping': successful_mapping,
        'failed_images': failed_images,
        'redshift_updated': redshift_success,
        'results': results
    }

def load_image_ids_from_file(file_path):
    """
    Load image IDs from various file formats
    
    Supports:
    - CSV files with 'image_id' column
    - Text files with one image ID per line
    - Excel files with 'image_id' column
    """
    file_path = Path(file_path)
    
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    if file_path.suffix.lower() == '.csv':
        df = pd.read_csv(file_path)
        if 'image_id' not in df.columns:
            raise ValueError("CSV file must have 'image_id' column")
        image_ids = df['image_id'].dropna().astype(int).tolist()
        
    elif file_path.suffix.lower() in ['.xlsx', '.xls']:
        df = pd.read_excel(file_path)
        if 'image_id' not in df.columns:
            raise ValueError("Excel file must have 'image_id' column")
        image_ids = df['image_id'].dropna().astype(int).tolist()
        
    elif file_path.suffix.lower() == '.txt':
        with open(file_path, 'r') as f:
            lines = f.readlines()
        image_ids = []
        for line in lines:
            line = line.strip()
            if line and line.isdigit():
                image_ids.append(int(line))
    else:
        raise ValueError("Supported file formats: .csv, .xlsx, .xls, .txt")
    
    print(f"Loaded {len(image_ids)} image IDs from {file_path}")
    return image_ids

# Example usage
if __name__ == "__main__":
    # Example: Replace image_id 1234 with its variant (123400000)
    image_id = 1234  # Replace with actual image ID
    
    result = process_image_replacement(image_id)
    
    if result['success']:
        print("Image replacement completed successfully!")
    else:
        print(f"Image replacement failed: {result['error']}")