# AI Variant Image Generator

A repackaged script that generates AI variant images from local image files using OpenAI's API. This script combines functionality from `generate_image.py` and `generate_variants.ipynb` but works with local files instead of database queries.

## Features

- ✅ Works with local image files (no database required)
- ✅ Supports multiple input images per variant
- ✅ Option to return temporary URL or download image file
- ✅ Customizable prompts and product highlights
- ✅ Token usage and cost tracking
- ✅ Command-line interface and Python API
- ✅ Single deal processing (no complex multiprocessing)

## Requirements

```bash
pip install openai python-dotenv requests
```

## Setup

1. Create a `.env` file in your project directory:
```
OPEN_AI_API_KEY=your_openai_api_key_here
```

2. Ensure you have the `generate_variant_from_files.py` script in your working directory.

## Usage

### Command Line Interface

#### Basic usage with image files:
```bash
python generate_variant_from_files.py image1.jpg image2.jpg --subject "Gaming Chair"
```

#### With product highlights:
```bash
python generate_variant_from_files.py *.jpg --subject "Office Desk" --highlights "Adjustable height" "Cable management" "USB charging ports"
```

#### Get temporary URL only (no download):
```bash
python generate_variant_from_files.py product.jpg --url-only --subject "Bluetooth Speaker"
```

#### Custom output filename:
```bash
python generate_variant_from_files.py images/*.jpg --output my_custom_variant.jpg --subject "Laptop Stand"
```

#### With custom prompt:
```bash
python generate_variant_from_files.py furniture.jpg --prompt "Create a minimalist lifestyle image with warm lighting"
```

### Python API

#### Basic usage:
```python
from generate_variant_from_files import generate_variant_from_files

# Generate and save variant
output_file, token_info = generate_variant_from_files(
    image_paths=["product1.jpg", "product2.jpg"],
    subject="Premium Headphones",
    highlights=["Wireless", "30-hour battery", "Noise cancellation"]
)

print(f"Generated: {output_file}")
print(f"Cost: {token_info['cost']}")
```

#### Get temporary URL only:
```python
temp_url = generate_variant_from_files(
    image_paths=["product.jpg"],
    subject="Smart Watch",
    return_url_only=True
)

print(f"Temporary URL: {temp_url}")
```

#### Custom prompt:
```python
custom_prompt = """
Create a luxurious product shot with dramatic lighting.
Place the product on a marble surface with soft shadows.
Use a dark, elegant background.
"""

output_file, token_info = generate_variant_from_files(
    image_paths=["jewelry.jpg"],
    custom_prompt=custom_prompt
)
```

## Function Parameters

### `generate_variant_from_files()`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `image_paths` | `List[str]` | Required | List of paths to local image files |
| `subject` | `str` | `"Premium Product"` | Product name for the prompt |
| `highlights` | `List[str]` | `None` | Product features/highlights |
| `output_file` | `str` | `None` | Output filename (auto-generated if None) |
| `return_url_only` | `bool` | `False` | Return temp URL instead of downloading |
| `custom_prompt` | `str` | `None` | Custom prompt (overrides default template) |

### Return Values

- **If `return_url_only=True`**: Returns temporary OpenAI URL (string)
- **If `return_url_only=False`**: Returns tuple of `(output_filename, token_info_dict)`

## Command Line Arguments

| Argument | Short | Description |
|----------|-------|-------------|
| `image_paths` | - | Paths to input image files (required) |
| `--subject` | `-s` | Product name/subject |
| `--highlights` | `-H` | Product highlights (multiple values) |
| `--output` | `-o` | Output filename |
| `--url-only` | `-u` | Return URL only, don't download |
| `--prompt` | `-p` | Custom prompt |

## Default Prompt Template

The script uses a comprehensive prompt template that includes:

- Product representation guidelines
- Scene and background requirements  
- Text overlay specifications
- Design constraints (e.g., keep bottom-right area clear)
- Highlight positioning rules

You can override this with the `--prompt` or `custom_prompt` parameter.

## Cost Information

The script automatically calculates and displays:
- Total tokens used
- Input/output token breakdown
- Estimated cost in USD

Pricing is based on OpenAI's current rates:
- Input text tokens: $5 per 1M tokens
- Input image tokens: $10 per 1M tokens  
- Output tokens: $40 per 1M tokens

## Examples

See `example_usage.py` for comprehensive examples including:
- Basic usage
- URL-only mode
- Custom prompts
- Programmatic batch processing

## Error Handling

The script includes robust error handling for:
- Missing or invalid image files
- OpenAI API errors
- File I/O issues
- Network connectivity problems

## Limitations

- Maximum 16 input images per variant (OpenAI API limit)
- Requires valid OpenAI API key with image generation credits
- Temporary URLs expire after some time
- Output format is always 1536x1024 JPG

## Differences from Original Scripts

| Original | This Script |
|----------|-------------|
| Database queries for deal data | Local image file input |
| Async multiprocessing | Single deal processing |
| S3 upload functionality | Local file or temp URL options |
| Fixed prompt from DB highlights | Customizable prompts and highlights |
| Complex variant management | Simple one-off generation | 