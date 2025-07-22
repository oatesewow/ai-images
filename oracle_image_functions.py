import oracledb
import os
import time
from PIL import Image

def processImage(source, dealId, n, folder, chatGPTFolder, is_url=True):
    if n > 10:
        return

    # # DB: get new image ID
    # connection = oracledb.connect(
    #     user="northcliffe",
    #     #password="432470f9wje5048",
    #     #dsn="nxt01-daily-deals.ccp4btiicvd7.eu-west-1.rds.amazonaws.com:1521/ANONDB"
    #     password="wlJEUu81We34",
    #     dsn="prod-gr-daily-deals.cs40xjobaadv.eu-west-1.rds.amazonaws.com:1521/WOWPRDDB"
    # )

    connection = oracledb.connect(
        user= os.getenv("NXT_DB_USER"),
        password= os.getenv("NXT_DB_PASSWORD"),
        dsn=os.getenv("NXT_DB_DSN")  # e.g., "dbhost.example.com:1521/XEPDB1"
    )
    cursor = connection.cursor()
    cursor.execute("SELECT product_image_seq.NEXTVAL FROM dual")
    new_image_id = cursor.fetchone()[0]
    connection.commit()
    cursor.close()
    connection.close()

    # Handle image source
    if is_url:
        img_data, ext, error = download_and_validate_image(source)
        if error:
            print(f"⚠️ Skipped ({error}): {source}")
            return
        img_data, ext = convert_to_jpg(img_data, ext)
        if img_data is None:
            print(source, ": convert_to_jpg_failed")
            return
        img = Image.open(img_data)
    else:
        try:
            img = Image.open(source)
        except Exception as e:
            print(f"⚠️ Skipped (cannot open image): {source} — {e}")
            return

    # Ensure RGB
    if img.mode != "RGB":
        img = img.convert("RGB")

    # Resize large images to fit ClipDrop limits
    img = resize_for_clipdrop(img)

    # Save original image
    ts = int(time.time())
    original_filename = f"{dealId}_{new_image_id}_original_{ts}.jpg"
    original_path = os.path.join(folder, original_filename)
    img.save(original_path, "JPEG")
    
    chatGPT_path = os.path.join(chatGPTFolder, original_filename)
    img.save(chatGPT_path, "JPEG")

    # Generate variants
    try:
        generate_variants(new_image_id, img, folder)
    except Exception as e:
        print(f"⚠️ Skipping image due to ClipDrop error: {e}")

    # Insert metadata into DB
    #resource_path = f"https://static01.nxtwowcher.co.uk/images/deal/{dealId}"
    resource_path = f"https://static.wowcher.co.uk/images/deal/{dealId}"
    # connection = oracledb.connect(
    #     user="northcliffe",
    #     #password="432470f9wje5048",
    #     #dsn="nxt01-daily-deals.ccp4btiicvd7.eu-west-1.rds.amazonaws.com:1521/ANONDB"
    #     password="wlJEUu81We34",
    #     dsn="prod-gr-daily-deals.cs40xjobaadv.eu-west-1.rds.amazonaws.com:1521/WOWPRDDB"
    # )

    connection = oracledb.connect(
        user= os.getenv("NXT_DB_USER"),
        password= os.getenv("NXT_DB_PASSWORD"),
        dsn=os.getenv("NXT_DB_DSN")  # e.g., "dbhost.example.com:1521/XEPDB1"
    )
    cursor = connection.cursor()
    cursor.execute("""
        INSERT INTO DEAL_VOUCHER_IMAGE (
            ID, DEAL_VOUCHER_ID, RESOURCE_PATH, STATUS_ID,
            FILE_NAME, CAPTION, POSITION, ALT_TAG,
            EXTENSION, HAS_IPHONE_IMG, CREATED_BY_USER_ID, CREATED_DATE
        ) VALUES (
            :new_image_id, :dealId, :resourcePath, 1,
            :originalFilename, :originalFilename, :n, :originalFilename,
            'jpg', 'Y', 60448730, CURRENT_TIMESTAMP
        )
    """, {
        "new_image_id": new_image_id,
        "dealId": dealId,
        "resourcePath": resource_path,
        "originalFilename": original_filename,
        "n": n,
    })
    connection.commit()
    cursor.close()
    connection.close()

    print("✅ Processed image:", source)
    
def processMain(source, dealId, folder):

    # DB: get new image ID
    # connection = oracledb.connect(
    #     user="northcliffe",
    #     #password="432470f9wje5048",
    #     #dsn="nxt01-daily-deals.ccp4btiicvd7.eu-west-1.rds.amazonaws.com:1521/ANONDB"
    #     password="wlJEUu81We34",
    #     dsn="prod-gr-daily-deals.cs40xjobaadv.eu-west-1.rds.amazonaws.com:1521/WOWPRDDB"
    # )

    connection = oracledb.connect(
        user= os.getenv("NXT_DB_USER"),
        password= os.getenv("NXT_DB_PASSWORD"),
        dsn=os.getenv("NXT_DB_DSN")  # e.g., "dbhost.example.com:1521/XEPDB1"
    )
    cursor = connection.cursor()
    cursor.execute("SELECT product_image_seq.NEXTVAL FROM dual")
    new_image_id = cursor.fetchone()[0]
    connection.commit()
    cursor.close()
    connection.close()
    
    try:
        img = Image.open(source)
    except Exception as e:
        print(f"⚠️ Skipped (cannot open image): {source} — {e}")
        return
        

    # Ensure RGB
    if img.mode != "RGB":
        img = img.convert("RGB")

    # Resize large images to fit ClipDrop limits
    img = resize_for_clipdrop(img)

    # Save original image
    ts = int(time.time())
    original_filename = f"{dealId}_{new_image_id}_original_{ts}.jpg"
    original_path = os.path.join(folder, original_filename)
    img.save(original_path, "JPEG")

    # Generate variants
    try:
        generate_variants(new_image_id, img, folder)
    except Exception as e:
        print(f"⚠️ Skipping image due to ClipDrop error: {e}")

    # Insert metadata into DB
    #resource_path = f"https://static01.nxtwowcher.co.uk/images/deal/{dealId}"
    resource_path = f"https://static.wowcher.co.uk/images/deal/{dealId}"
    # connection = oracledb.connect(
    #     user="northcliffe",
    #     #password="432470f9wje5048",
    #     #dsn="nxt01-daily-deals.ccp4btiicvd7.eu-west-1.rds.amazonaws.com:1521/ANONDB"
    #     password="wlJEUu81We34",
    #     dsn="prod-gr-daily-deals.cs40xjobaadv.eu-west-1.rds.amazonaws.com:1521/WOWPRDDB"
    # )

    connection = oracledb.connect(
        user= os.getenv("NXT_DB_USER"),
        password= os.getenv("NXT_DB_PASSWORD"),
        dsn=os.getenv("NXT_DB_DSN")  # e.g., "dbhost.example.com:1521/XEPDB1"
    )
    cursor = connection.cursor()
    
    cursor.execute("""
        UPDATE DEAL_VOUCHER_IMAGE 
        SET POSITION = POSITION + 1
        WHERE DEAL_VOUCHER_ID = :dealId
    """, {
        "dealId": dealId
    })
    
    cursor.execute("""
        INSERT INTO DEAL_VOUCHER_IMAGE (
            ID, DEAL_VOUCHER_ID, RESOURCE_PATH, STATUS_ID,
            FILE_NAME, CAPTION, POSITION, ALT_TAG,
            EXTENSION, HAS_IPHONE_IMG, CREATED_BY_USER_ID, CREATED_DATE
        ) VALUES (
            :new_image_id, :dealId, :resourcePath, 1,
            :originalFilename, :originalFilename, :n, :originalFilename,
            'jpg', 'Y', 18282217, CURRENT_TIMESTAMP
        )
    """, {
        "new_image_id": new_image_id,
        "dealId": dealId,
        "resourcePath": resource_path,
        "originalFilename": original_filename,
        "n": 0,
    })
    connection.commit()
    cursor.close()
    connection.close()

    print("✅ Processed image:", source)

def get_new_oracle_image_id():
    """
    Get a new image ID from Oracle sequence
    """
    connection = oracledb.connect(
        user=os.getenv("NXT_DB_USER"),
        password=os.getenv("NXT_DB_PASSWORD"),
        dsn=os.getenv("NXT_DB_DSN")
    )
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
        
        for obj in response['Contents']:
            key = obj['Key']
            
            # Check if this file contains the original image ID
            if original_pattern in key:
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

def insert_variant_image_oracle(deal_id, new_image_id, variant_s3_url, s3_client, bucket_name):
    """
    Insert the variant image as position 0 in Oracle and copy the variant to S3
    """
    try:
        # First, copy the variant image from its current location to the new image ID location
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
        
        # Create new key for the variant image (position 0)
        new_variant_key = f"images/deal/{deal_id}/{new_image_id}.jpg"
        
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
        
        # Create filename for Oracle record
        import time
        ts = int(time.time())
        variant_filename = f"{deal_id}_{new_image_id}_variant_{ts}.jpg"
        
        # Connect to Oracle and update positions, then insert
        connection = oracledb.connect(
            user=os.getenv("NXT_DB_USER"),
            password=os.getenv("NXT_DB_PASSWORD"),
            dsn=os.getenv("NXT_DB_DSN")
        )
        cursor = connection.cursor()
        
        # First, update all existing positions for this deal
        cursor.execute("""
            UPDATE DEAL_VOUCHER_IMAGE 
            SET POSITION = POSITION + 1
            WHERE DEAL_VOUCHER_ID = :dealId
        """, {
            "dealId": deal_id
        })
        
        # Insert the new variant image at position 0
        resource_path = f"https://static.wowcher.co.uk/images/deal/{deal_id}"
        
        cursor.execute("""
            INSERT INTO DEAL_VOUCHER_IMAGE (
                ID, DEAL_VOUCHER_ID, RESOURCE_PATH, STATUS_ID,
                FILE_NAME, CAPTION, POSITION, ALT_TAG,
                EXTENSION, HAS_IPHONE_IMG, CREATED_BY_USER_ID, CREATED_DATE
            ) VALUES (
                :new_image_id, :dealId, :resourcePath, 1,
                :variantFilename, :variantFilename, 0, :variantFilename,
                'jpg', 'Y', 18282217, CURRENT_TIMESTAMP
            )
        """, {
            "new_image_id": new_image_id,
            "dealId": deal_id,
            "resourcePath": resource_path,
            "variantFilename": variant_filename
        })
        
        connection.commit()
        cursor.close()
        connection.close()
        
        final_variant_url = f"https://{bucket_name}/{new_variant_key}"
        print(f"✅ Successfully processed variant for deal {deal_id}: {final_variant_url}")
        
        return {
            'new_image_id': new_image_id,
            'variant_url': final_variant_url,
            'success': True
        }
        
    except Exception as e:
        print(f"Error inserting variant image for deal {deal_id}: {str(e)}")
        return {
            'new_image_id': new_image_id,
            'variant_url': None,
            'success': False,
            'error': str(e)
        }

def process_approved_variant(deal_id, original_image_id, variant_s3_url, s3_client, bucket_name):
    """
    Complete workflow for processing an approved variant:
    1. Get new Oracle image ID
    2. Copy existing files
    3. Insert variant image at position 0
    """
    try:
        print(f"Processing approved variant for deal {deal_id}...")
        
        # Step 1: Get new Oracle image ID
        new_image_id = get_new_oracle_image_id()
        print(f"Got new Oracle image ID: {new_image_id}")
        
        # Step 2: Copy existing files
        copied_files = copy_existing_s3_files(s3_client, bucket_name, deal_id, original_image_id, new_image_id)
        print(f"Copied {len(copied_files)} existing files")
        
        # Step 3: Insert variant image
        result = insert_variant_image_oracle(deal_id, new_image_id, variant_s3_url, s3_client, bucket_name)
        
        return {
            'deal_id': deal_id,
            'original_image_id': original_image_id,
            'new_image_id': new_image_id,
            'copied_files': copied_files,
            'variant_result': result,
            'success': result['success']
        }
        
    except Exception as e:
        print(f"Error in complete workflow for deal {deal_id}: {str(e)}")
        return {
            'deal_id': deal_id,
            'original_image_id': original_image_id,
            'new_image_id': None,
            'copied_files': [],
            'variant_result': {'success': False, 'error': str(e)},
            'success': False
        }

    print("✅ Processed image:", source)
