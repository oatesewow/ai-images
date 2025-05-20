import pandas as pd
import os
import psycopg2
from dotenv import load_dotenv
import requests
import base64
from openai import OpenAI
from io import BytesIO
import argparse

# Load environment variables
load_dotenv()

# Initialize OpenAI client
client = OpenAI(api_key=os.getenv('OPEN_AI_API_KEY'))

def get_deal_data(deal_id):
    # Establish connection to Redshift
    conn = psycopg2.connect(
        host=os.environ.get("REDSHIFT_HOST", "bi-redshift.intwowcher.co.uk"),
        port=os.environ.get("REDSHIFT_PORT", "5439"),
        dbname=os.environ.get("REDSHIFT_DBNAME", "wowdwhprod"),
        user=os.environ.get("REDSHIFT_USER", "jenkins"),
        password=os.environ.get("REDSHIFT_PASSWORD", "9SDy1ffdfTV7")
    )

    # Get email subject
    email_subject_query = """
    SELECT email_subject 
    FROM wowdwhprod.real.deal_voucher
    WHERE id = %s
    """

    with conn.cursor() as cur:
        cur.execute(email_subject_query, (deal_id,))
        email_subject_result = cur.fetchone()
        email_subject = email_subject_result[0] if email_subject_result else "Deal"

    # Get image URLs and extract extension information
    image_query = """
    SELECT 
        'https://static.wowcher.co.uk/images/deal/' || deal_voucher_id || '/' || id || '.' || extension AS image_url,
        extension
    FROM wowdwhprod.real.deal_voucher_image
    WHERE deal_voucher_id = %s
    ORDER BY position
    LIMIT 10
    """

    with conn.cursor() as cur:
        cur.execute(image_query, (deal_id,))
        image_results = cur.fetchall()
        image_urls = [row[0] for row in image_results]
        extensions = [row[1] for row in image_results]
        # Get the first extension to use as default
        original_extension = extensions[0] if extensions else "png"

    # Get highlights
    highlights_query = """
    SELECT highlight 
    FROM wowdwhprod.real.deal_voucher_highlight 
    WHERE deal_voucher_id = %s
    limit 3
    """

    with conn.cursor() as cur:
        cur.execute(highlights_query, (deal_id,))
        highlights_results = cur.fetchall()
        highlights = [row[0] for row in highlights_results]

    conn.close()

    # Build the prompt
    formatted_highlights = "\n".join([f"• {h}" for h in highlights]) if highlights else ""

    prompt = f"""
Create ONE high-resolution hero image advertising **{email_subject}**.

Final image must contain **zero spelling mistakes**.  

1. **Source images** – You have multiple angles.  
   • Accurately represent the product; do **not** invent new colours or features.  
   • If variants exist, PICK ONE colour and keep it consistent.

2. **Scene & background**  
   • Place the product in a realistic, aspirational environment that makes sense for its use.  
   • Adjust lighting and depth of field so the product is the clear focal point.  
   • Background must not overpower or obscure the product.

3. **Infographic & text elements**  
    • Do **not** repeat the headline anywhere else in the artwork.  
    • Any additional text must be limited to the 2-4 call-outs listed below.
    
   • Overlay 2-4 concise call-outs drawn from these highlights:  
     {formatted_highlights}  
   • Position all call-outs **outside** the bottom-right 20% of the frame.

4. **Design constraints**  
   • Keep bottom-right area completely free of any graphics or text.  
   • Maintain 4 px padding around all text boxes.  
   • No brand logos unless provided in the source images.
    """

    return {
        'prompt': prompt,
        'image_urls': image_urls,
        'original_extension': original_extension
    }

def download_image_to_file(url, filename):
    response = requests.get(url)
    if response.status_code == 200:
        with open(filename, 'wb') as f:
            f.write(response.content)
        return filename
    else:
        raise Exception(f"Failed to download image from {url}")

def generate_image(deal_id, output_filename=None):
    print("Starting image generation process...")
    deal_data = get_deal_data(deal_id)
    prompt = deal_data['prompt']
    image_urls = deal_data['image_urls']
    original_extension = deal_data['original_extension']
    
    # If no output filename provided, create one with the original extension
    if output_filename is None:
        output_filename = f"generated_image_{deal_id}.{original_extension}"
    else:
        # If output filename provided but no extension, use the original extension
        if '.' not in output_filename:
            output_filename = f"{output_filename}.{original_extension}"
        # If different extension provided, keep the provided one
    
    if not image_urls:
        raise Exception("No images found for this deal")
    print(f"Found {len(image_urls)} images. Downloading up to 16 images...")
    # Download up to 16 images for the edit API
    image_files = []
    temp_filenames = []
    for idx, url in enumerate(image_urls[:16]):
        temp_filename = f"temp_image_{idx}.png"
        download_image_to_file(url, temp_filename)
        temp_filenames.append(temp_filename)
        image_files.append(open(temp_filename, "rb"))
    print("Calling OpenAI API to edit images...")
    result = client.images.edit(
        model="gpt-image-1",
        image=image_files,
        prompt=prompt,
        size="1536x1024",
        quality="high",
        background="auto",
        n=1
    )
    print("Received response from OpenAI API. Decoding and saving image...")
    image_base64 = result.data[0].b64_json
    image_bytes = base64.b64decode(image_base64)
    with open(output_filename, "wb") as f:
        f.write(image_bytes)
    for f in image_files:
        f.close()
    print(f"Saved generated image to {output_filename}")
    print("Token usage details:")
    print(f"Total tokens: {result.usage.total_tokens}")
    print(f"Input tokens: {result.usage.input_tokens}")
    print(f"Output tokens: {result.usage.output_tokens}")
    print(f"Input tokens details: {result.usage.input_tokens_details}")
    # Calculate cost based on pricing
    input_text_tokens = result.usage.input_tokens_details.text_tokens
    input_image_tokens = result.usage.input_tokens_details.image_tokens
    output_tokens = result.usage.output_tokens
    cost = (input_text_tokens * 5 + input_image_tokens * 10 + output_tokens * 40) / 1000000
    print(f"Cost: ${cost:.6f}")
    
    return output_filename, result

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate an image using OpenAI API for a given deal ID.")
    parser.add_argument("deal_id", type=int, help="The deal ID to generate an image for.")
    parser.add_argument("output_file", nargs="?", type=str, help="Optional output filename. If not provided, creates one with original extension.")
    args = parser.parse_args()
    try:
        output_file = args.output_file if args.output_file else None
        generate_image(args.deal_id, output_file)
    except Exception as e:
        print(f"Error: {str(e)}") 