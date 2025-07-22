# Replace Position Zero Variant

This module provides functionality to replace the main image (position 0) of a deal with its variant, while intelligently rearranging all other image positions.

## Overview

The system downloads a variant image (identified by adding "00000" to the original image ID), creates a new Oracle image ID, uploads the variant with the new ID, and rearranges the image positions according to specific rules.

## Position Shuffling Logic

When you replace an image, the following position changes occur:

1. **New variant image** → Goes to **position 0** (becomes the main image)
2. **Original position 0 image** → Moves to **position 2**
3. **Position 1 image** → **Stays at position 1** (unchanged)
4. **All other images** → **Shift down by 1 position**

### Example

**Before:**
- Position 0: Image A (ID: 1234567)
- Position 1: Image B (ID: 1234568)
- Position 2: Image C (ID: 1234569)
- Position 3: Image D (ID: 1234570)

**After processing image_id 1234567:**
- Position 0: **Variant of Image A** (ID: 9876543 - new Oracle ID)
- Position 1: Image B (ID: 1234568) - unchanged
- Position 2: **Image A** (ID: 1234567) - moved from position 0
- Position 3: Image C (ID: 1234569) - shifted down from position 2
- Position 4: Image D (ID: 1234570) - shifted down from position 3

## Files

- `replace_position_zero_variant.py` - Main functionality
- `example_usage.py` - Usage examples and batch processing
- `README_replace_position_zero.md` - This documentation

## Dependencies

Make sure you have the required dependencies in your `requirements.txt`:

```
pandas
boto3
python-dotenv
oracledb
psycopg2-binary
requests
tqdm
openpyxl  # For Excel file support
```

## Environment Variables

Ensure your `.env` file contains the following variables:

```bash
# AWS Configuration
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
S3_BUCKET_NAME=static.wowcher.co.uk

# Oracle Configuration
ORACLE_USER=your_oracle_username
ORACLE_PASSWORD=your_oracle_password
ORACLE_DSN=your_oracle_dsn

# Redshift Configuration
REDSHIFT_HOST=your_redshift_host
REDSHIFT_PORT=your_redshift_port
REDSHIFT_DBNAME=your_redshift_database
REDSHIFT_USER=your_redshift_username
REDSHIFT_PASSWORD=your_redshift_password
```

## Usage

### Single Image Processing

```python
from replace_position_zero_variant import process_image_replacement_with_redshift

# Process a single image with Redshift update
image_id = 1234567
result = process_image_replacement_with_redshift(image_id, update_redshift=True)

if result['success']:
    print(f"Success! New image ID: {result['new_image_id']}")
    print(f"Deal ID: {result['deal_id']}")
    print(f"New image URL: {result['final_url']}")
    
    if result.get('redshift_updated'):
        print("✅ Redshift status updated (status=3, exit_test_ts set)")
    else:
        print("⚠️ Redshift status not updated")
else:
    print(f"Failed: {result['error']}")
```

### Batch Processing with Redshift

```python
from replace_position_zero_variant import (
    process_image_replacement, 
    update_redshift_status_batch
)

image_ids = [1234567, 1234568, 1234569]
successful_mapping = {}

# Process all images first
for image_id in image_ids:
    result = process_image_replacement(image_id)
    if result['success']:
        successful_mapping[image_id] = result['new_image_id']

# Then update Redshift for all successful ones with fast batch update
if successful_mapping:
    update_redshift_status_batch(successful_mapping)
```

### Command Line Usage

```bash
# Process a single image
python example_usage.py 1234567

# Process multiple images (sequential - edit image_ids list first)
python example_usage.py batch

# Process multiple images (multi-threaded - edit image_ids list first) 
python example_usage.py batch-workers

# Process multiple images from file (interactive)
python example_usage.py batch-file

# Run default example
python example_usage.py
```

### Batch Processing Options

#### 1. Sequential Batch (Original)
Edit the `image_ids` list in `batch_image_replacement()` function:

```python
def batch_image_replacement():
    image_ids = [
        1234567,  # Your actual image IDs
        1234568,
        1234569
    ]
```

#### 2. Multi-threaded Batch (Recommended)
Edit the `image_ids` list in `batch_image_replacement_with_workers()` function:

```python
def batch_image_replacement_with_workers():
    image_ids = [
        1234567,  # Your actual image IDs
        1234568,
        1234569
    ]
    # Processes with 25 workers by default
```

#### 3. From File (Most Flexible)
Create a file with your image IDs and use:

```bash
python example_usage.py batch-file
# Then enter file path when prompted
```

Supported file formats:
- **CSV**: Must have `image_id` column
- **Excel**: Must have `image_id` column  
- **Text**: One image ID per line

Example files are provided: `example_image_ids.csv` and `example_image_ids.txt`

### Performance Optimizations

#### Multi-threaded Image Processing
The multi-threaded batch processing (`batch-workers`) provides significant performance improvements:

- **Default**: 25 workers (can be adjusted)
- **Speed**: 3-5x faster than sequential processing
- **Progress**: Real-time progress bar with success/failure counts
- **I/O Optimized**: Perfect for S3 downloads/uploads and database operations

Performance comparison for 100 images:
- Sequential: ~10-15 minutes
- Multi-threaded (25 workers): ~3-5 minutes

#### Ultra-Fast Redshift Updates
Uses temporary table approach for maximum performance:

```sql
CREATE TEMP TABLE temp_image_updates (
    original_image_id BIGINT,
    new_variant_image_id BIGINT
);

INSERT INTO temp_image_updates VALUES 
    (1234567, 9876543), 
    (1234568, 9876544), 
    -- all images in single INSERT
    ;

UPDATE temp.opt_image_variants 
SET status = 3, 
    exit_test_ts = CURRENT_TIMESTAMP,
    variant_image_id = temp_updates.new_variant_image_id
FROM temp_image_updates temp_updates
WHERE temp.opt_image_variants.original_image_id = temp_updates.original_image_id 
AND temp.opt_image_variants.status = 1;
```

**Performance Results:**
- **Any batch size**: ~1 second (scales efficiently)
- **Old individual queries**: 1000 images = ~30 seconds
- **New temp table approach**: 1000 images = ~1 second ⚡

### Multi-threaded Function

```python
from replace_position_zero_variant import process_batch_with_workers

# Process with custom worker count
result = process_batch_with_workers(
    image_ids=[1234567, 1234568, 1234569],
    max_workers=50,  # Adjust based on your system
    update_redshift=True
)

# Returns detailed results
print(f"Success rate: {result['successful_count']}/{result['total_processed']}")
```

## Workflow Details

### Step 1: Download Variant Image
- Looks up the deal_id for the given image_id in Oracle
- Downloads the variant image from S3 using pattern: `images/deal/{deal_id}/{image_id}00000.jpg`
- Stores in temporary directory

### Step 2: Get New Oracle Image ID
- Calls Oracle sequence `deal_voucher_image_seq.NEXTVAL` (or falls back to `product_image_seq.NEXTVAL`)
- Uses the proper sequence for the DEAL_VOUCHER_IMAGE table
- This ensures no cache conflicts

### Step 3: Upload to S3
- Uploads the downloaded variant to S3 with the new image ID
- URL pattern: `images/deal/{deal_id}/{new_image_id}.jpg`
- Sets appropriate content type and cache control headers

### Step 4: Update Oracle Database
- Moves all existing images to temporary positions to avoid conflicts
- Applies the position shuffling logic
- Inserts new image record at position 0
- Uses database transaction for safety

### Step 5: Update Redshift (Optional)
- Updates `temp.opt_image_variants` table
- Sets `status = 3` (indicating image replacement completed)
- Sets `exit_test_ts = CURRENT_TIMESTAMP`
- Sets `variant_image_id = new_oracle_id` (the new Oracle ID that was generated)
- Only updates records where `status = 1` (active testing)

## Error Handling

The system includes comprehensive error handling:

- **Download failures**: If variant image doesn't exist in S3
- **Oracle connection issues**: Database connectivity problems
- **Upload failures**: S3 upload problems
- **Position conflicts**: Database constraint violations
- **Cleanup**: Temporary files are always cleaned up

## Return Values

The `process_image_replacement()` function returns a dictionary:

```python
{
    'success': True,  # or False
    'original_image_id': 1234567,
    'new_image_id': 9876543,
    'deal_id': 12345,
    'final_url': 'https://static.wowcher.co.uk/images/deal/12345/9876543.jpg',
    'redshift_updated': True,  # only present when using _with_redshift function
    'error': None  # or error message if success=False
}
```

## Redshift Functions

### Single Image Redshift Update
```python
from replace_position_zero_variant import update_redshift_status_single

# Update status only
success = update_redshift_status_single(1234567)

# Update status and variant_image_id
success = update_redshift_status_single(1234567, new_variant_image_id=9876543)
```

### Batch Redshift Update
```python
from replace_position_zero_variant import update_redshift_status_batch

# Ultra-fast batch update using temporary table approach
image_mapping = {
    1234567: 9876543,  # original_id: new_oracle_id
    1234568: 9876544,
    1234569: 9876545
}
success = update_redshift_status_batch(image_mapping)

# Works efficiently for any batch size (1 image or 10,000 images)
large_mapping = {i: i+1000000 for i in range(1234567, 1235567)}  # 1000 images
success = update_redshift_status_batch(large_mapping)
```

### Complete Workflow with Redshift
```python
from replace_position_zero_variant import process_image_replacement_with_redshift

# Process image and update Redshift in one call
result = process_image_replacement_with_redshift(1234567, update_redshift=True)
```

## Differences from process_approved_variants.py

While both systems handle image variants, this new system:

1. **Targets specific image replacement** instead of batch approval processing
2. **Implements position shuffling logic** for maintaining image order
3. **Downloads from existing variants** instead of generating new ones
4. **Focuses on single-image workflows** with batch capability
5. **Uses temporary file handling** for the download/upload process

## Safety Features

- **Database transactions**: All Oracle operations are wrapped in transactions
- **Temporary position handling**: Avoids position conflicts during updates
- **File cleanup**: Temporary downloads are automatically removed
- **Error rollback**: Failed operations don't leave partial changes
- **Validation**: Checks for image existence before processing

## Troubleshooting

### Common Issues

1. **"No deal found for image_id"**
   - The image_id doesn't exist in Oracle
   - Check if the image_id is correct

2. **"Error downloading variant image"**
   - The variant image (image_id + "00000") doesn't exist in S3
   - Check if the variant was created properly

3. **Oracle connection errors**
   - Check your Oracle credentials in `.env`
   - Verify network connectivity to Oracle database

4. **S3 permission errors**
   - Verify AWS credentials in `.env`
   - Check S3 bucket permissions

### Debug Mode

Add debug prints by modifying the functions to include more verbose logging if needed.

## Support

This system is designed to work alongside the existing `process_approved_variants.py` functionality. Both can be used independently based on your workflow needs. 