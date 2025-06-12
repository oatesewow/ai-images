import pandas as pd
import boto3
import requests
from dotenv import load_dotenv
import os
import psycopg2
from datetime import datetime
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

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
    Prepare data for Redshift upload
    """
    print("Preparing data for Redshift...")
    
    # Create a new dataframe with the required columns
    redshift_df = pd.DataFrame()
    
    # Map columns from source to destination
    redshift_df['deal_voucher_id'] = df['id']
    redshift_df['claid_prompt'] = ''  # Blank as requested
    redshift_df['status'] = 1  # Set to 1 as requested
    redshift_df['original_image_id'] = df['image_id_pos_0']
    
    # Create variant image ID (original ID * 100000)
    redshift_df['variant_image_id'] = df['image_id_pos_0'].astype(int) * 100000
    
    redshift_df['batch_name'] = BATCH_NAME
    redshift_df['enter_test_ts'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    redshift_df['exit_test_ts'] = None
    redshift_df['list_name'] = 'imgv_list_wow_uk'
    
    # Add the s3_url for reference (this won't be uploaded to Redshift)
    redshift_df['s3_url'] = df['s3_url']
    redshift_df['original_url'] = df['image_url_pos_0']
    
    print(f"Prepared {len(redshift_df)} rows for Redshift")
    return redshift_df

def process_single_image(args):
    """
    Process a single image - copy S3 object to new location with variant ID
    """
    index, row, s3_client, bucket_name = args
    try:
        # Get the generated image URL (s3_url = variant image)
        variant_url = row['s3_url']
        deal_id = str(row['deal_voucher_id'])
        original_image_id = str(row['original_image_id'])
        variant_image_id = str(row['variant_image_id'])
        
        # Skip if no variant URL
        if pd.isna(variant_url):
            print(f"Skipping row {index}: No variant URL")
            return index, None, None
        
        # Parse the S3 URL to get source bucket and key
        # Handle different S3 URL formats
        if variant_url.startswith('https://'):
            # Format: https://bucket.s3.amazonaws.com/key or https://bucket/key
            if '.s3.amazonaws.com/' in variant_url:
                # https://bucket.s3.amazonaws.com/key
                parts = variant_url.replace('https://', '').split('.s3.amazonaws.com/', 1)
                source_bucket = parts[0]
                source_key = parts[1]
            else:
                # https://bucket/key  
                parts = variant_url.replace('https://', '').split('/', 1)
                source_bucket = parts[0]
                source_key = parts[1] if len(parts) > 1 else ''
        elif variant_url.startswith('s3://'):
            # Format: s3://bucket/key
            parts = variant_url.replace('s3://', '').split('/', 1)
            source_bucket = parts[0]
            source_key = parts[1] if len(parts) > 1 else ''
        else:
            print(f"Unsupported S3 URL format for deal {deal_id}: {variant_url}")
            return index, None, None
        
        # Create new key with variant ID
        new_key = f"images/deal/{deal_id}/{variant_image_id}.jpg"
        
        # Copy object within S3 (much more efficient than download/upload)
        copy_source = {
            'Bucket': source_bucket,
            'Key': source_key
        }
        
        s3_client.copy_object(
            CopySource=copy_source,
            Bucket=bucket_name,
            Key=new_key,
            MetadataDirective='REPLACE',
            ContentType='image/jpeg',
            CacheControl='no-cache'
        )
        
        # Return the final URL
        final_url = f"https://{bucket_name}/{new_key}"
        return index, final_url, True
        
    except Exception as e:
        print(f"Error processing deal {deal_id}: {str(e)}")
        return index, None, False

def copy_approved_variants_to_s3(df):
    """
    Copy approved variants to S3 with new variant IDs
    """
    print("Connecting to S3...")
    
    try:
        # Connect to S3
        s3_client = boto3.client('s3', **AWS_CONFIG)
        bucket_name = S3_CONFIG['bucket_name']
        
        # Add new columns for tracking
        df['final_s3_url'] = None
        df['processed_status'] = False
    
        # Create arguments for each row
        args_list = [(idx, row, s3_client, bucket_name) 
                    for idx, row in df.iterrows()]
    
        # Process images concurrently using ThreadPoolExecutor
        success_count = 0
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(process_single_image, args) 
                      for args in args_list]
            
            with tqdm(total=len(df), desc="Copying Variants within S3") as pbar:
                for future in as_completed(futures):
                    idx, final_url, status = future.result()
                    if final_url:
                        df.loc[idx, 'final_s3_url'] = final_url
                        df.loc[idx, 'processed_status'] = status
                        success_count += 1
                    pbar.update(1)
    
        print(f"Successfully copied {success_count} of {len(df)} variants to S3")
        
        # Save the processed data
        df.to_csv(CSV_OUTPUT_FILE, index=False)
        print(f"Saved processed data to {CSV_OUTPUT_FILE}")
        
        return df
        
    except Exception as e:
        print(f"Error copying to S3: {str(e)}")
        return df

def upload_to_s3_for_redshift(df):
    """
    Upload prepared data to S3 for Redshift COPY
    """
    print("Preparing data for Redshift...")
    
    try:
        # Create a new dataframe with only the columns needed for Redshift
        cols_for_redshift = [
            'deal_voucher_id', 'claid_prompt', 'status', 'original_image_id', 
            'variant_image_id', 'batch_name', 'enter_test_ts', 'exit_test_ts', 'list_name'
        ]
        
        upload_df = df[cols_for_redshift].copy()
        
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
    Update the test image list in the API
    """
    print("Updating image list in API...")
    
    try:
        # Initialize connection
        connection = psycopg2.connect(**REDSHIFT_CONFIG)
        cursor = connection.cursor()
        
        # Get all active original image IDs
        cursor.execute("""
            SELECT original_image_id 
            FROM temp.opt_image_variants
            WHERE status = 1
            GROUP BY original_image_id
        """)
        
        # Format image IDs for API
        image_ids = [f":{str(row[0])}" for row in cursor.fetchall()]
        
        print(f"Found {len(image_ids)} unique images to add to list")
        
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
        
        print(f"Successfully updated test list with {len(image_ids)} images")
        return True
        
    except Exception as e:
        print(f"Error updating test list: {e}")
        return False
        
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()

# Example notebook usage:
"""
# Step 1: Load and filter approved images
csv_file = "All500Approved.csv"
approved_df = load_and_filter_approved_images(csv_file)

# Step 2: Prepare data for Redshift
redshift_df = prepare_for_redshift(approved_df)

# Step 3: Copy variants to S3 with new variant IDs
processed_df = copy_approved_variants_to_s3(redshift_df)

# Step 4: Upload prepared data to S3 for Redshift
s3_url = upload_to_s3_for_redshift(processed_df)

# Step 5: Copy data from S3 to Redshift
success = copy_s3_to_redshift(s3_url)

# Step 6: Update the image list in the API
update_image_list()
""" 