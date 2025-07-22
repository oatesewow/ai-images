import pandas as pd
import boto3
import requests
from dotenv import load_dotenv
import os
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

def check_existing_variants(deal_id, image_id):
    """
    Check which variants already exist in S3
    """
    s3_client = boto3.client('s3', **AWS_CONFIG)
    bucket_name = S3_CONFIG['bucket_name']
    
    existing_variants = set()
    missing_variants = []
    
    for suffix in TARGET_SIZES.keys():
        key = f"images/deal/{deal_id}/{image_id}{suffix}.jpg"
        
        try:
            s3_client.head_object(Bucket=bucket_name, Key=key)
            existing_variants.add(suffix)
        except s3_client.exceptions.ClientError:
            missing_variants.append(suffix)
    
    return existing_variants, missing_variants

def download_main_image(deal_id, image_id, temp_dir):
    """
    Download the main image from S3
    """
    try:
        s3_client = boto3.client('s3', **AWS_CONFIG)
        bucket_name = S3_CONFIG['bucket_name']
        
        # Try to download the main image
        main_key = f"images/deal/{deal_id}/{image_id}.jpg"
        temp_file_path = os.path.join(temp_dir, f"{image_id}_source.jpg")
        
        s3_client.download_file(bucket_name, main_key, temp_file_path)
        
        # Load as PIL Image
        original_image = Image.open(temp_file_path)
        
        return temp_file_path, original_image
        
    except Exception as e:
        print(f"Error downloading main image for deal {deal_id}, image {image_id}: {str(e)}")
        raise

def upload_missing_variants_to_s3(variant_files, deal_id, image_id, missing_suffixes):
    """
    Upload only the missing image variants to S3
    """
    try:
        s3_client = boto3.client('s3', **AWS_CONFIG)
        bucket_name = S3_CONFIG['bucket_name']
        uploaded_urls = {}
        
        for suffix in missing_suffixes:
            if suffix not in variant_files:
                continue
                
            local_file_path = variant_files[suffix]
            new_key = f"images/deal/{deal_id}/{image_id}{suffix}.jpg"
            
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
        
        return uploaded_urls
        
    except Exception as e:
        print(f"Error uploading missing variants to S3: {str(e)}")
        raise

def process_single_image(deal_id, image_id):
    """
    Process a single image to generate missing variants
    """
    temp_dir = None
    
    try:
        # Create temporary directory
        temp_dir = tempfile.mkdtemp()
        
        # Check what variants already exist
        existing_variants, missing_variants = check_existing_variants(deal_id, image_id)
        
        if not missing_variants:
            return {
                'success': True,
                'deal_id': deal_id,
                'image_id': image_id,
                'message': 'All variants already exist',
                'missing_count': 0,
                'generated_count': 0
            }
        
        # Download main image
        temp_file_path, original_image = download_main_image(deal_id, image_id, temp_dir)
        
        # Generate all variants
        variant_files = generate_variants(image_id, original_image, temp_dir)
        
        # Upload only missing variants
        uploaded_urls = upload_missing_variants_to_s3(variant_files, deal_id, image_id, missing_variants)
        
        return {
            'success': True,
            'deal_id': deal_id,
            'image_id': image_id,
            'existing_variants': list(existing_variants),
            'missing_count': len(missing_variants),
            'generated_count': len(uploaded_urls),
            'uploaded_urls': uploaded_urls
        }
        
    except Exception as e:
        return {
            'success': False,
            'deal_id': deal_id,
            'image_id': image_id,
            'error': str(e)
        }
        
    finally:
        # Clean up temporary directory
        if temp_dir and os.path.exists(temp_dir):
            import shutil
            shutil.rmtree(temp_dir)

def process_csv_batch(csv_file_path, max_workers=10):
    """
    Process a CSV file with deal_id and image_id columns
    """
    # Load CSV
    df = pd.read_csv(csv_file_path)
    
    # Validate required columns
    required_cols = ['deal_id', 'image_id']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"CSV missing required columns: {missing_cols}")
    
    # Remove duplicates and NaN values
    df = df.dropna(subset=required_cols).drop_duplicates(subset=required_cols)
    
    print(f"Processing {len(df)} unique image records from CSV...")
    print("=" * 60)
    
    results = []
    successful_count = 0
    failed_count = 0
    skipped_count = 0
    total_variants_generated = 0
    
    # Process in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all jobs
        future_to_row = {
            executor.submit(process_single_image, row['deal_id'], row['image_id']): row
            for _, row in df.iterrows()
        }
        
        # Process completed jobs with progress bar
        with tqdm(total=len(df), desc="Processing Images") as pbar:
            for future in as_completed(future_to_row):
                row = future_to_row[future]
                
                try:
                    result = future.result()
                    results.append(result)
                    
                    if result['success']:
                        if result['generated_count'] > 0:
                            successful_count += 1
                            total_variants_generated += result['generated_count']
                        else:
                            skipped_count += 1
                    else:
                        failed_count += 1
                    
                    pbar.set_postfix({
                        "Success": successful_count, 
                        "Skipped": skipped_count, 
                        "Failed": failed_count,
                        "Total Images": total_variants_generated
                    })
                        
                except Exception as e:
                    failed_count += 1
                    results.append({
                        'success': False,
                        'deal_id': row['deal_id'],
                        'image_id': row['image_id'],
                        'error': str(e)
                    })
                    pbar.set_postfix({
                        "Success": successful_count, 
                        "Skipped": skipped_count, 
                        "Failed": failed_count,
                        "Total Images": total_variants_generated
                    })
                
                pbar.update(1)
    
    # Print summary
    print("\n" + "=" * 60)
    print("BATCH PROCESSING SUMMARY")
    print("=" * 60)
    print(f"Total processed: {len(df)}")
    print(f"Successfully generated variants: {successful_count}")
    print(f"Already had all variants: {skipped_count}")
    print(f"Failed: {failed_count}")
    print(f"Total variant images generated: {total_variants_generated}")
    
    # Show failed items if any
    failed_results = [r for r in results if not r['success']]
    if failed_results:
        print(f"\nFailed items:")
        for failure in failed_results[:10]:  # Show first 10 failures
            print(f"  Deal {failure['deal_id']}, Image {failure['image_id']}: {failure['error']}")
        if len(failed_results) > 10:
            print(f"  ... and {len(failed_results) - 10} more")
    
    return {
        'total_processed': len(df),
        'successful_count': successful_count,
        'skipped_count': skipped_count,
        'failed_count': failed_count,
        'total_variants_generated': total_variants_generated,
        'results': results
    }

def main():
    """
    Main function for command line usage
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate missing image variants from CSV')
    parser.add_argument('csv_file', help='Path to CSV file with deal_id and image_id columns')
    parser.add_argument('--workers', type=int, default=10, help='Number of parallel workers (default: 10)')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.csv_file):
        print(f"Error: CSV file not found: {args.csv_file}")
        return
    
    try:
        results = process_csv_batch(args.csv_file, max_workers=args.workers)
        print(f"\nProcessing complete! Generated {results['total_variants_generated']} variant images.")
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main() 