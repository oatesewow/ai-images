# Image Variants Management

This document explains how to use the updated image replacement system that automatically generates all required image variants.

## Updated Script: `replace_position_zero_variant.py`

The main script has been updated to automatically generate all required image variants when creating new images.

### What's New

When you replace an image, the script now:
1. Downloads the source variant image (e.g., `123400000.jpg`)
2. Generates **all 14 image variants** with different sizes:
   - Main image (777×520)
   - `-cashback-promo` (126×90)
   - `-email` (640×428)  
   - `-thumb` (105×70)
   - `-promo` (172×115)
   - `-user` (50×50)
   - `-deal-bonus` (278×182)
   - `-iphone-medium` (151×106)
   - `-iphone-small` (80×53)
   - `-iphone-promo` (640×428)
   - `-iphone-thumb` (210×140)
   - `-travel-main` (777×520)
   - `-2-per-row` (470×315)
   - `-3-per-row` (310×210)
3. Uploads all variants to S3
4. Updates Oracle database positions

### Usage Examples

```python
# Single image replacement
from replace_position_zero_variant import process_image_replacement

result = process_image_replacement(1234567)
if result['success']:
    print(f"Generated {result['variants_count']} variants")
    print(f"New image ID: {result['new_image_id']}")
```

```python
# Batch processing
from replace_position_zero_variant import process_batch_with_workers

image_ids = [1234567, 1234568, 1234569]
results = process_batch_with_workers(image_ids, max_workers=25)
```

## New Script: `generate_missing_variants.py`

This script is for **retroactively** generating missing variants for existing images that don't have all their size variants.

### When to Use

- You have existing images in S3 that are missing some variant sizes
- You want to ensure all images have complete variant sets
- You need to fix images that were uploaded before the automatic variant generation

### CSV Format Required

Your CSV file must have these columns:
- `deal_id`: The deal ID where the image belongs
- `image_id`: The main image ID (without suffixes)

Example CSV:
```csv
deal_id,image_id
12345,1709952
12346,1642547
12347,1642558
```

### Usage Examples

#### Command Line Usage
```bash
# Process a CSV file
python generate_missing_variants.py /path/to/your/images.csv

# With custom number of workers
python generate_missing_variants.py /path/to/your/images.csv --workers 15
```

#### Python Usage
```python
from generate_missing_variants import process_csv_batch

# Process CSV file
results = process_csv_batch('images_to_fix.csv', max_workers=10)

print(f"Generated {results['total_variants_generated']} variant images")
print(f"Successfully processed: {results['successful_count']}")
print(f"Already complete: {results['skipped_count']}")
print(f"Failed: {results['failed_count']}")
```

#### Single Image Processing
```python
from generate_missing_variants import process_single_image

# Check and generate missing variants for one image
result = process_single_image(deal_id=12345, image_id=1709952)

if result['success']:
    print(f"Generated {result['generated_count']} missing variants")
    print(f"Already had: {len(result['existing_variants'])} variants")
```

### What It Does

For each image in your CSV, the script:
1. **Checks S3** to see which variants already exist
2. **Skips** images that already have all variants
3. **Downloads** the main image from S3
4. **Generates** only the missing variant sizes
5. **Uploads** only the missing variants to S3
6. **Reports** progress and results

### Performance

- **Parallel processing**: Uses multiple workers for faster processing
- **Smart checking**: Only generates variants that don't already exist
- **Progress tracking**: Shows real-time progress with counts
- **Error handling**: Continues processing if individual images fail

### Output

The script provides detailed progress tracking:
```
Processing 150 unique image records from CSV...
Processing Images: 100%|██████████| 150/150 [02:45<00:00,  0.91it/s] ✅: 120 ⏭️: 25 ❌: 5 🖼️: 1,680

============================================================
BATCH PROCESSING SUMMARY
============================================================
Total processed: 150
Successfully generated variants: 120
Already had all variants: 25
Failed: 5
Total variant images generated: 1,680
```

### Error Handling

- **Failed downloads**: Images that can't be downloaded from S3
- **Missing images**: Images that don't exist at the specified S3 path
- **Upload failures**: Network or permission issues with S3
- **Processing errors**: Invalid image formats or processing issues

Failed items are reported with specific error messages for debugging.

## Environment Variables

Both scripts require these environment variables:

```bash
# AWS Configuration
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
S3_BUCKET_NAME=static.wowcher.co.uk

# Oracle Database (for main script only)
ORACLE_USER=your_oracle_user
ORACLE_PASSWORD=your_oracle_password  
ORACLE_DSN=your_oracle_dsn

# Redshift (for main script only)
REDSHIFT_HOST=your_redshift_host
REDSHIFT_PORT=5439
REDSHIFT_DBNAME=your_db_name
REDSHIFT_USER=your_redshift_user
REDSHIFT_PASSWORD=your_redshift_password
```

## Performance Tips

1. **Batch Size**: Use appropriate worker counts based on your system
   - Main script: 15-25 workers for image replacement
   - Variants script: 8-15 workers for variant generation

2. **Network**: Ensure good internet connection for S3 operations

3. **Memory**: Each worker uses temporary storage for image processing

4. **Monitoring**: Watch the progress bars and error messages for issues

## File Structure

After processing, your S3 structure will look like:
```
images/deal/12345/
  ├── 1709952.jpg                 # Main image (777×520)
  ├── 1709952-cashback-promo.jpg  # Cashback promo (126×90)
  ├── 1709952-email.jpg           # Email version (640×428)
  ├── 1709952-thumb.jpg           # Thumbnail (105×70)
  ├── 1709952-promo.jpg           # Promo (172×115)
  ├── 1709952-user.jpg            # User avatar (50×50)
  ├── 1709952-deal-bonus.jpg      # Deal bonus (278×182)
  ├── 1709952-iphone-medium.jpg   # iPhone medium (151×106)
  ├── 1709952-iphone-small.jpg    # iPhone small (80×53)
  ├── 1709952-iphone-promo.jpg    # iPhone promo (640×428)
  ├── 1709952-iphone-thumb.jpg    # iPhone thumb (210×140)
  ├── 1709952-travel-main.jpg     # Travel main (777×520)
  ├── 1709952-2-per-row.jpg       # 2 per row (470×315)
  └── 1709952-3-per-row.jpg       # 3 per row (310×210)
``` 