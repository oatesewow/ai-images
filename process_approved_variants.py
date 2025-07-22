import pandas as pd
import boto3
import requests
from dotenv import load_dotenv
import os
import psycopg2
from datetime import datetime
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import oracledb

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

API_CONFIG = {
    'secret': os.getenv('API_SECRET')
}

# Oracle configuration
ORACLE_CONFIG = {
    'user': os.getenv("ORACLE_USER"),
    'password': os.getenv("ORACLE_PASSWORD"),
    'dsn': os.getenv("ORACLE_DSN")
}

# Constants
BATCH_NAME = "OPEN AI Images"
MAX_WORKERS = 25
CSV_OUTPUT_FILE = 'processed_approved_variants.csv'
REDSHIFT_UPLOAD_FILE = 'redshift_upload_data.csv'

def load_and_filter_approved_images(csv_filename):
    """
    Load CSV and filter only approved images
    """
    print(f"Loading data from {csv_filename}...")
    
    try:
        df = pd.read_csv(csv_filename)
        print(f"Loaded {len(df)} rows from CSV")
        
        # Filter only approved images
        approved_df = df[df['review_result'] == 'approved'].copy()
        print(f"Found {len(approved_df)} approved images")
        
        # Ensure required columns exist
        required_columns = ['id', 'image_id_pos_0', 's3_url', 'image_url_pos_0']
        for col in required_columns:
            if col not in approved_df.columns:
                raise ValueError(f"Required column '{col}' not found in CSV")
        
        return approved_df
        
    except Exception as e:
        print(f"Error loading CSV: {str(e)}")
        return None

def prepare_for_redshift(df):
    """
    Prepare data for Redshift upload - variant_image_id will be new Oracle ID * 100000
    """
    print("Preparing data structure for Redshift...")
    
    # Create a new dataframe with the required columns
    redshift_df = pd.DataFrame()
    
    # Map columns from source to destination
    redshift_df['deal_voucher_id'] = df['id']
    redshift_df['claid_prompt'] = ''  # Blank as requested
    redshift_df['status'] = 1  # Set to 1 as requested
    redshift_df['original_image_id'] = df['image_id_pos_0']
    
    # Initialize variant_image_id as None - will be populated as new_oracle_id * 100000
    redshift_df['variant_image_id'] = None
    
    redshift_df['batch_name'] = BATCH_NAME
    redshift_df['enter_test_ts'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    redshift_df['exit_test_ts'] = None
    redshift_df['list_name'] = 'imgv_list_wow_uk'
    
    # Add additional tracking columns
    redshift_df['s3_url'] = df['s3_url']
    redshift_df['original_url'] = df['image_url_pos_0']
    redshift_df['processed_status'] = False
    redshift_df['final_s3_url'] = None
    redshift_df['new_oracle_id'] = None  # Track the new Oracle ID
    
    print(f"Prepared {len(redshift_df)} rows for processing")
    return redshift_df

def process_single_approved_variant(args):
    """
    Process a single approved variant using the new Oracle workflow
    """
    index, row, s3_client, bucket_name = args
    
    try:
        deal_id = str(row['deal_voucher_id'])
        original_image_id = row['original_image_id']
        variant_s3_url = row['s3_url']
        
        # Skip if no variant URL
        if pd.isna(variant_s3_url):
            print(f"Skipping row {index}: No variant URL")
            return index, None, None, None, False
        
        # Process the approved variant using the new Oracle workflow
        result = process_approved_variant(deal_id, original_image_id, variant_s3_url, s3_client, bucket_name)
        
        if result['success']:
            return index, result['new_image_id'], result['variant_image_id'], result['variant_result']['variant_url'], True
        else:
            print(f"Failed to process deal {deal_id}: {result.get('error', 'Unknown error')}")
            return index, None, None, None, False
            
    except Exception as e:
        print(f"Error processing deal {deal_id}: {str(e)}")
        return index, None, None, None, False

def process_approved_variants_with_oracle(df):
    """
    Process approved variants using the new Oracle workflow
    """
    print("Processing approved variants with Oracle...")
    
    try:
        # Connect to S3
        s3_client = boto3.client('s3', **AWS_CONFIG)
        bucket_name = S3_CONFIG['bucket_name']
    
        # Create arguments for each row
        args_list = [(idx, row, s3_client, bucket_name) 
                    for idx, row in df.iterrows()]
    
        # Process variants concurrently using ThreadPoolExecutor
        success_count = 0
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(process_single_approved_variant, args) 
                      for args in args_list]
            
            with tqdm(total=len(df), desc="Processing Approved Variants with Oracle") as pbar:
                for future in as_completed(futures):
                    idx, new_oracle_id, variant_image_id, final_url, status = future.result()
                    if status:
                        df.loc[idx, 'new_oracle_id'] = new_oracle_id
                        df.loc[idx, 'variant_image_id'] = variant_image_id
                        df.loc[idx, 'final_s3_url'] = final_url
                        df.loc[idx, 'processed_status'] = status
                        success_count += 1
                    pbar.update(1)
    
        print(f"Successfully processed {success_count} of {len(df)} variants")
        
        # Save the processed data
        df.to_csv(CSV_OUTPUT_FILE, index=False)
        print(f"Saved processed data to {CSV_OUTPUT_FILE}")
        
        return df
        
    except Exception as e:
        print(f"Error processing variants: {str(e)}")
        return df

# Keep the old function as backup but rename it
def copy_approved_variants_to_s3_old(df):
    """
    OLD METHOD: Copy approved variants to S3 with new variant IDs (multiplication method)
    This is kept for reference but should not be used with the new Oracle workflow
    
    This function used to multiply original_image_id by 100000 to create variant_image_id,
    but this caused cache problems. The new workflow gets fresh Oracle image IDs instead.
    """
    print("⚠️ WARNING: This is the OLD multiplication method. Use process_approved_variants_with_oracle() instead.")
    return df

def upload_to_s3_for_redshift(df):
    """
    Upload prepared data to S3 for Redshift COPY - only successfully processed rows
    """
    print("Preparing data for Redshift...")
    
    try:
        # Filter only successfully processed rows
        processed_df = df[df['processed_status'] == True].copy()
        
        if len(processed_df) == 0:
            print("No successfully processed variants to upload to Redshift")
            return None
            
        print(f"Found {len(processed_df)} successfully processed variants for Redshift")
        
        # Create a new dataframe with only the columns needed for Redshift
        cols_for_redshift = [
            'deal_voucher_id', 'claid_prompt', 'status', 'original_image_id', 
            'variant_image_id', 'batch_name', 'enter_test_ts', 'exit_test_ts', 'list_name'
        ]
        
        upload_df = processed_df[cols_for_redshift].copy()
        
        # Update original_image_id to use the new Oracle ID (the actual ID now in Oracle)
        upload_df['original_image_id'] = processed_df['new_oracle_id']
        
        # Fix decimal issues - convert float columns to integers
        upload_df['deal_voucher_id'] = upload_df['deal_voucher_id'].astype(int)
        upload_df['status'] = upload_df['status'].astype(int)
        upload_df['original_image_id'] = upload_df['original_image_id'].astype(int)
        upload_df['variant_image_id'] = upload_df['variant_image_id'].astype(int)
        
        # Save to local CSV first
        upload_df.to_csv(REDSHIFT_UPLOAD_FILE, index=False)
        print(f"Saved Redshift data to {REDSHIFT_UPLOAD_FILE}")
        
        # Connect to S3
        s3_client = boto3.client('s3', **AWS_CONFIG)
        bucket_name = S3_CONFIG['bucket_name']
        
        # Upload to S3
        s3_key = f'temp/ai_image_variants_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'
        
        with open(REDSHIFT_UPLOAD_FILE, 'rb') as data:
            s3_client.upload_fileobj(data, bucket_name, s3_key)
        
        s3_url = f'https://{bucket_name}/{s3_key}'
        print(f"Uploaded data to S3: {s3_url}")
        
        return s3_url
    
    except Exception as e:
        print(f"Error uploading to S3: {str(e)}")
        return None

def copy_s3_to_redshift(s3_url):
    """
    Copy data from S3 to Redshift
    """
    print("Copying data to Redshift...")
    
    try:
        connection = psycopg2.connect(**REDSHIFT_CONFIG)
        cursor = connection.cursor()
        
        # Convert S3 URL to format needed for COPY command
        s3_path = s3_url.replace(f'https://{S3_CONFIG["bucket_name"]}/', f's3://{S3_CONFIG["bucket_name"]}/')
        
        # COPY command with explicit column mapping
        copy_command = f"""
        COPY temp.opt_image_variants(
            deal_voucher_id,
            claid_prompt,
            status,
            original_image_id,
            variant_image_id,
            batch_name, 
            enter_test_ts,
            exit_test_ts,
            list_name
        )
        FROM '{s3_path}'
        ACCESS_KEY_ID '{AWS_CONFIG["aws_access_key_id"]}'
        SECRET_ACCESS_KEY '{AWS_CONFIG["aws_secret_access_key"]}'
        CSV
        IGNOREHEADER 1
        ACCEPTINVCHARS AS '^'
        MAXERROR 10;
        """
        
        cursor.execute(copy_command)
        connection.commit()
        
        cursor.execute(f"SELECT COUNT(*) FROM temp.opt_image_variants WHERE batch_name = '{BATCH_NAME}'")
        row_count = cursor.fetchone()[0]
        
        print(f"Successfully copied {row_count} rows to Redshift table")
        return True
        
    except Exception as e:
        print(f"Error copying to Redshift: {str(e)}")
        if 'connection' in locals() and connection:
            connection.rollback()
        return False
        
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()

def update_image_list():
    """
    Update the test image list in the API with variant image IDs (new_oracle_id * 100000)
    """
    print("Updating image list in API...")
    
    try:
        # Initialize connection
        connection = psycopg2.connect(**REDSHIFT_CONFIG)
        cursor = connection.cursor()
        
        # Get all active variant image IDs (not original image IDs)
        cursor.execute("""
            SELECT original_image_id 
            FROM temp.opt_image_variants
            WHERE status = 1 AND batch_name = %s
            GROUP BY original_image_id
        """, (BATCH_NAME,))
        
        # Format image IDs for API - using the NEW variant image IDs
        image_ids = [f":{str(row[0])}" for row in cursor.fetchall()]
        
        print(f"Found {len(image_ids)} unique NEW variant images to add to list")
        
        # Set up API headers
        headers = {
            "x-wowsecret": API_CONFIG['secret'],
            "Content-Type": "application/json"
        }
        
        # Update the API with the list
        response = requests.post(
            "https://www.wowcher.co.uk/deal-variant-db/deal/set?dv_id=imgv_list_wow_uk",
            headers=headers,
            json=image_ids
        )
        response.raise_for_status()
        
        print(f"Successfully updated test list with {len(image_ids)} NEW variant images")
        return True
        
    except Exception as e:
        print(f"Error updating test list: {e}")
        return False
        
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()

# Oracle Functions
def get_new_oracle_image_id():
    """
    Get a new image ID from Oracle sequence
    """
    connection = oracledb.connect(**ORACLE_CONFIG)
    cursor = connection.cursor()
    cursor.execute("SELECT product_image_seq.NEXTVAL FROM dual")
    new_image_id = cursor.fetchone()[0]
    connection.commit()
    cursor.close()
    connection.close()
    return new_image_id

def copy_existing_s3_files(s3_client, bucket_name, deal_id, original_image_id, new_image_id):
    """
    Copy all existing S3 files from original_image_id to new_image_id pattern
    Excludes variant files (those with 5 zeros pattern like 123400000.jpg and files with "_variant" in name)
    """
    try:
        # List all objects with the original image ID pattern
        prefix = f"images/deal/{deal_id}/"
        
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix
        )
        
        if 'Contents' not in response:
            print(f"No existing files found for deal {deal_id}")
            return []
        
        copied_files = []
        original_pattern = str(original_image_id)
        new_pattern = str(new_image_id)
        variant_pattern = f"{original_image_id}00000"  # 5-zero variant pattern
        
        for obj in response['Contents']:
            key = obj['Key']
            
            # Check if this file contains the original image ID
            if original_pattern in key:
                # Skip if this is a variant file (contains the 5-zero pattern)
                if variant_pattern in key:
                    print(f"Skipping 5-zero variant file: {key}")
                    continue
                
                # Skip if this is a variant file (contains "_variant" in filename)
                if "_variant" in key:
                    print(f"Skipping _variant file: {key}")
                    continue
                
                # Create new key by replacing original image ID with new image ID
                new_key = key.replace(original_pattern, new_pattern)
                
                # Copy the object
                copy_source = {
                    'Bucket': bucket_name,
                    'Key': key
                }
                
                s3_client.copy_object(
                    CopySource=copy_source,
                    Bucket=bucket_name,
                    Key=new_key,
                    MetadataDirective='COPY'
                )
                
                copied_files.append({
                    'original': f"https://{bucket_name}/{key}",
                    'new': f"https://{bucket_name}/{new_key}"
                })
                
                print(f"Copied: {key} -> {new_key}")
        
        return copied_files
        
    except Exception as e:
        print(f"Error copying existing files for deal {deal_id}: {str(e)}")
        return []

def insert_base_oracle_records(deal_id, original_image_id, new_image_id):
    """
    Replace existing Oracle records with new image ID
    Updates existing records to use new_image_id, keeping same positions
    Now handles DEAL_VOUCHER_PRODUCT_IMAGE foreign key constraint
    """
    connection = None
    cursor = None
    
    try:
        connection = oracledb.connect(**ORACLE_CONFIG)
        cursor = connection.cursor()
        
        # Get all existing records for the original image ID
        cursor.execute("""
            SELECT RESOURCE_PATH, FILE_NAME, CAPTION, POSITION, ALT_TAG, EXTENSION, HAS_IPHONE_IMG
            FROM DEAL_VOUCHER_IMAGE 
            WHERE DEAL_VOUCHER_ID = :dealId AND ID = :originalImageId
            ORDER BY POSITION
        """, {
            "dealId": deal_id,
            "originalImageId": original_image_id
        })
        
        existing_records = cursor.fetchall()
        
        if not existing_records:
            print(f"No existing records found for original image ID {original_image_id}")
            return False
        
        # STEP 1: First, create new parent records with the new image ID
        # This ensures the parent key exists before we update child records
        for record in existing_records:
            resource_path, file_name, caption, position, alt_tag, extension, has_iphone_img = record
            
            # Create new filename with new image ID
            new_file_name = file_name.replace(str(original_image_id), str(new_image_id))
            new_caption = caption.replace(str(original_image_id), str(new_image_id)) if caption else new_file_name
            new_alt_tag = alt_tag.replace(str(original_image_id), str(new_image_id)) if alt_tag else new_file_name
            
            # Insert new record with new Oracle ID but same position
            cursor.execute("""
                INSERT INTO DEAL_VOUCHER_IMAGE (
                    ID, DEAL_VOUCHER_ID, RESOURCE_PATH, STATUS_ID,
                    FILE_NAME, CAPTION, POSITION, ALT_TAG,
                    EXTENSION, HAS_IPHONE_IMG, CREATED_BY_USER_ID, CREATED_DATE
                ) VALUES (
                    :new_image_id, :dealId, :resourcePath, 1,
                    :fileName, :caption, :position, :altTag,
                    :extension, :hasIphoneImg, 18282217, CURRENT_TIMESTAMP
                )
            """, {
                "new_image_id": new_image_id,  # Use new Oracle ID
                "dealId": deal_id,
                "resourcePath": resource_path,
                "fileName": new_file_name,
                "caption": new_caption,
                "position": position,  # Keep original position
                "altTag": new_alt_tag,
                "extension": extension,
                "hasIphoneImg": has_iphone_img
            })
        
        # STEP 2: Now handle child records in DEAL_VOUCHER_PRODUCT_IMAGE
        # Check if there are any child records that reference the original image ID
        cursor.execute("""
            SELECT COUNT(*) 
            FROM DEAL_VOUCHER_PRODUCT_IMAGE 
            WHERE DEAL_VOUCHER_IMAGE_ID = :originalImageId
        """, {
            "originalImageId": original_image_id
        })
        
        child_count = cursor.fetchone()[0]
        
        if child_count > 0:
            print(f"Found {child_count} child records in DEAL_VOUCHER_PRODUCT_IMAGE for image ID {original_image_id}")
            
            # Update child records to reference the new image ID (now the parent exists)
            cursor.execute("""
                UPDATE DEAL_VOUCHER_PRODUCT_IMAGE 
                SET DEAL_VOUCHER_IMAGE_ID = :newImageId
                WHERE DEAL_VOUCHER_IMAGE_ID = :originalImageId
            """, {
                "newImageId": new_image_id,
                "originalImageId": original_image_id
            })
            
            print(f"✅ Updated {child_count} child records to reference new image ID {new_image_id}")
        
        # STEP 3: Finally, delete the old parent records (now safe since child records point to new parent)
        cursor.execute("""
            DELETE FROM DEAL_VOUCHER_IMAGE 
            WHERE DEAL_VOUCHER_ID = :dealId AND ID = :originalImageId
        """, {
            "dealId": deal_id,
            "originalImageId": original_image_id
        })
        
        connection.commit()
        
        print(f"✅ Replaced {len(existing_records)} Oracle records with new ID {new_image_id}, keeping original positions")
        if child_count > 0:
            print(f"✅ Also updated {child_count} DEAL_VOUCHER_PRODUCT_IMAGE references")
        
        return True
        
    except Exception as e:
        print(f"Error replacing Oracle records: {str(e)}")
        if connection:
            connection.rollback()
        return False
        
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def copy_variant_to_s3(deal_id, new_image_id, variant_s3_url, s3_client, bucket_name):
    """
    Copy the variant image to S3 with new_image_id * 100000 pattern
    NOTE: Variant does NOT get inserted into Oracle - only exists in S3 for testing
    """
    try:
        # Calculate variant image ID (new Oracle ID + 5 zeros)
        variant_image_id = new_image_id * 100000
        
        # Parse the variant S3 URL
        if variant_s3_url.startswith('https://'):
            clean_url = variant_s3_url.split('?')[0]
            if '.s3.amazonaws.com/' in clean_url:
                parts = clean_url.replace('https://', '').split('.s3.amazonaws.com/', 1)
                source_bucket = parts[0]
                source_key = parts[1]
            else:
                parts = clean_url.replace('https://', '').split('/', 1)
                source_bucket = parts[0]
                source_key = parts[1] if len(parts) > 1 else ''
        elif variant_s3_url.startswith('s3://'):
            parts = variant_s3_url.replace('s3://', '').split('/', 1)
            source_bucket = parts[0]
            source_key = parts[1] if len(parts) > 1 else ''
        else:
            raise ValueError(f"Unsupported S3 URL format: {variant_s3_url}")
        
        # Create new key for the variant image (using variant_image_id with 5 zeros)
        new_variant_key = f"images/deal/{deal_id}/{variant_image_id}.jpg"
        
        # Copy the variant image to the new location
        copy_source = {
            'Bucket': source_bucket,
            'Key': source_key
        }
        
        s3_client.copy_object(
            CopySource=copy_source,
            Bucket=bucket_name,
            Key=new_variant_key,
            MetadataDirective='REPLACE',
            ContentType='image/jpeg',
            CacheControl='no-cache'
        )
        
        final_variant_url = f"https://{bucket_name}/{new_variant_key}"
        print(f"✅ Successfully copied variant to S3 for deal {deal_id}: {final_variant_url}")
        print(f"   (Variant ID {variant_image_id} exists only in S3, not in Oracle)")
        
        return {
            'variant_image_id': variant_image_id,
            'variant_url': final_variant_url,
            'success': True
        }
        
    except Exception as e:
        print(f"Error copying variant image for deal {deal_id}: {str(e)}")
        return {
            'variant_image_id': None,
            'variant_url': None,
            'success': False,
            'error': str(e)
        }

def process_approved_variant(deal_id, original_image_id, variant_s3_url, s3_client, bucket_name):
    """
    Complete workflow for processing an approved variant:
    1. Get new Oracle image ID
    2. Copy existing files to new image ID pattern
    3. Insert base Oracle records with new image ID
    4. Copy variant image to new_image_id * 100000 pattern (S3 only)
    """
    try:
        print(f"Processing approved variant for deal {deal_id}...")
        
        # Step 1: Get new Oracle image ID
        new_image_id = get_new_oracle_image_id()
        print(f"Got new Oracle image ID: {new_image_id}")
        
        # Step 2: Copy existing files from original to new image ID
        copied_files = copy_existing_s3_files(s3_client, bucket_name, deal_id, original_image_id, new_image_id)
        print(f"Copied {len(copied_files)} existing files")
        
        # Step 3: Insert base Oracle records with new image ID
        base_oracle_success = insert_base_oracle_records(deal_id, original_image_id, new_image_id)
        
        # Step 4: Copy variant image to new_image_id * 100000 pattern (S3 only)
        result = copy_variant_to_s3(deal_id, new_image_id, variant_s3_url, s3_client, bucket_name)
        
        return {
            'deal_id': deal_id,
            'original_image_id': original_image_id,
            'new_image_id': new_image_id,
            'variant_image_id': result.get('variant_image_id'),
            'copied_files': copied_files,
            'base_oracle_success': base_oracle_success,
            'variant_result': result,
            'success': result['success'] and base_oracle_success
        }
        
    except Exception as e:
        print(f"Error in complete workflow for deal {deal_id}: {str(e)}")
        return {
            'deal_id': deal_id,
            'original_image_id': original_image_id,
            'new_image_id': None,
            'variant_image_id': None,
            'copied_files': [],
            'base_oracle_success': False,
            'variant_result': {'success': False, 'error': str(e)},
            'success': False
        }

# Updated example notebook usage:
"""
# NEW ORACLE-BASED WORKFLOW

# Step 1: Load and filter approved images
csv_file = "All500Approved.csv"
approved_df = load_and_filter_approved_images(csv_file)

# Step 2: Prepare data structure for processing
redshift_df = prepare_for_redshift(approved_df)

# Step 3: Process variants using Oracle workflow (gets new IDs, copies files, adds variants)
processed_df = process_approved_variants_with_oracle(redshift_df)

# Step 4: Upload successfully processed data to S3 for Redshift
s3_url = upload_to_s3_for_redshift(processed_df)

# Step 5: Copy data from S3 to Redshift
if s3_url:
    success = copy_s3_to_redshift(s3_url)

# Step 6: Update the image list in the API with variant image IDs
if success:
    update_image_list()

# Summary of what this new workflow does:
# 1. For each approved variant (original_image_id = 1234):
#    a. Gets a new Oracle image ID (e.g., 5678)
#    b. Copies ALL existing S3 files for deal that contain "1234" to new files with "5678"
#    c. Handles DEAL_VOUCHER_PRODUCT_IMAGE foreign key constraint by updating child records first
#    d. Inserts new Oracle records for base images using new image ID (5678)
#    e. Copies the approved variant from s3_url to: images/deal/{deal_id}/{567800000}.jpg
#    f. Inserts new Oracle record for the variant with ID 567800000 (5678 * 100000)
# 2. This solves the cache problem because we get completely fresh Oracle image IDs
# 3. All existing images are preserved with new IDs, and the variant is added as additional image
# 4. The variant uses the familiar multiplication pattern but with fresh Oracle base IDs
# 5. Both base images and variants get proper Oracle database records with fresh IDs
# 6. IMPORTANT: The function now handles the DEAL_VOUCHER_PRODUCT_IMAGE_FK2 constraint by:
#    - First creating new parent records in DEAL_VOUCHER_IMAGE with new image ID
#    - Then updating child records in DEAL_VOUCHER_PRODUCT_IMAGE to reference the new parent
#    - Finally deleting the old parent records from DEAL_VOUCHER_IMAGE
#    - This prevents both ORA-02291 (parent key not found) and ORA-02292 (child record found) errors
""" 