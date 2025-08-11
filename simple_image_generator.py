import requests
import base64
import tempfile
import os
from PIL import Image
import psycopg2
from openai import OpenAI

# Initialize OpenAI client
client = OpenAI(api_key=os.getenv('OPEN_AI_API_KEY'))

def get_deal_data_for_image(deal_id):
    """Get deal data needed for image generation"""
    # Establish connection to Redshift with hardcoded credentials
    conn = psycopg2.connect(
        host="bi-redshift.intwowcher.co.uk",
        port=5439,
        dbname="wowdwhprod",
        user="jenkins",
        password="9SDy1ffdfTV7"
    )

    # Get email subject
    email_subject_query = """
    SELECT deal_product 
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
        extension,
        id as image_id
    FROM wowdwhprod.real.deal_voucher_image
    WHERE deal_voucher_id = %s
    ORDER BY position
    LIMIT 1;
    """
    with conn.cursor() as cur:
        cur.execute(image_query, (deal_id,))
        image_results = cur.fetchall()
        image_urls = [row[0] for row in image_results]
        extensions = [row[1] for row in image_results]
        image_ids = [row[2] for row in image_results]
        original_extension = extensions[0] if extensions else "png"
        original_image_id = image_ids[0] if image_ids else None

    conn.close()

    return {
        'email_subject': email_subject,
        'image_urls': image_urls,
        'original_extension': original_extension,
        'original_image_id': original_image_id
    }

def generate_variant_from_file(image_file_path, email_subject, output_filename=None):
    """
    Generate a variant image from a file using OpenAI.
    
    Args:
        image_file_path (str): Path to the input image file
        email_subject (str): Email subject to use in the prompt
        output_filename (str, optional): Output filename. If None, auto-generates.
    
    Returns:
        str: Path to the generated image file
    """
    # Hardcoded prompt with email_subject placeholder
    prompt = f"Can you remove the background and only give me a white background studio photo of the {email_subject}"
    
    # Auto-generate output filename if not provided
    if output_filename is None:
        safe_subject = "".join(c for c in email_subject if c.isalnum() or c in (' ', '-', '_')).rstrip()
        safe_subject = safe_subject.replace(' ', '_')
        output_filename = f"variant_{safe_subject}.png"
    
    # Read the image file
    with open(image_file_path, "rb") as image_file:
        # Call OpenAI API
        result = client.images.edit(
            model="gpt-image-1",
            image=[image_file],
            prompt=prompt,
            size="1536x1024",
            quality="high",
            background="transparent",
            output_format="png",
            input_fidelity="high",
            n=1
        )
    
    # Process the response
    image_base64 = result.data[0].b64_json
    image_bytes = base64.b64decode(image_base64)
    
    # Save the generated image
    with open(output_filename, "wb") as f:
        f.write(image_bytes)
    
    print(f"Generated image saved as: {output_filename}")
    print(f"Token usage - Total: {result.usage.total_tokens}")
    
    return output_filename

def download_first_image_from_redshift(deal_id):
    """Download the first image for a deal from Redshift"""
    deal_data = get_deal_data_for_image(deal_id)
    
    if not deal_data['image_urls']:
        raise Exception(f"No images found for deal {deal_id}")
    
    # Get the first image URL
    first_image_url = deal_data['image_urls'][0]
    
    # Download the image
    response = requests.get(first_image_url)
    if response.status_code == 200:
        return response.content, deal_data['original_extension']
    else:
        raise Exception(f"Failed to download image from {first_image_url}")

def generate_variant_from_deal_id(deal_id, output_filename=None):
    """
    Convenience function for testing - downloads first image from deal_id and generates variant.
    
    Args:
        deal_id (int): Deal ID to download image from
        output_filename (str, optional): Output filename. If None, auto-generates.
    
    Returns:
        str: Path to the generated image file
    """
    # Get deal data to extract email_subject
    deal_data = get_deal_data_for_image(deal_id)
    email_subject = deal_data['email_subject']
    
    # Download first image
    image_bytes, extension = download_first_image_from_redshift(deal_id)
    
    # Save to temporary file
    temp_filename = f"temp_deal_{deal_id}.{extension}"
    with open(temp_filename, "wb") as f:
        f.write(image_bytes)
    
    try:
        # Generate variant using the main function
        result = generate_variant_from_file(temp_filename, email_subject, output_filename)
        return result
    finally:
        # Clean up temporary file
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

# Example usage:
if __name__ == "__main__":
    # For production use - with file and email_subject
    # result = generate_variant_from_file("path/to/image.jpg", "Amazing Product Deal")
    
    # For testing - with deal_id
    # result = generate_variant_from_deal_id(32692546)
    pass 