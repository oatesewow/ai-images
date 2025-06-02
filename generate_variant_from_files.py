import os
import base64
import requests
from typing import List, Optional, Union
from openai import OpenAI
from dotenv import load_dotenv
# Load environment variables
load_dotenv()
# Initialize OpenAI client
client = OpenAI(api_key=os.getenv('OPEN_AI_API_KEY'))
def generate_variant(
    image_filepaths: List[str],
    email_subject: str,
    highlights: List[str],
    download_filepath: Optional[str] = None
) -> str:
    """
    Generate an AI variant image from local image files.
    Args:
        image_filepaths: List of paths to local image files
        email_subject: Product/email subject for the prompt
        highlights: List of product highlights/features
        download_filepath: Optional filepath to save image. If None, auto-generates filename
    Returns:
        Returns filepath where image was saved (str)
    """
    # Validate input files
    valid_paths = []
    for path in image_filepaths:
        if os.path.exists(path):
            valid_paths.append(path)
        else:
            print(f"Warning: File not found: {path}")
    if not valid_paths:
        raise Exception("No valid image files found")
    # Limit to 16 images for API
    valid_paths = valid_paths[:16]
    # Build prompt with highlights
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
    print(f"Processing {len(valid_paths)} images...")
    # Open image files
    image_files = []
    try:
        for path in valid_paths:
            image_files.append(open(path, "rb"))
        # Call OpenAI API
        print("Calling OpenAI API...")
        result = client.images.edit(
            model="gpt-image-1",
            image=image_files,
            prompt=prompt,
            size="1536x1024",
            quality="high",
            background="auto",
            n=1
        )
        # Get base64 data
        image_base64 = result.data[0].b64_json
        # Decode base64 image
        image_bytes = base64.b64decode(image_base64)
        # Determine output filename
        if download_filepath is None:
            safe_subject = "".join(c for c in email_subject if c.isalnum() or c in (' ', '-', '_')).rstrip()
            safe_subject = safe_subject.replace(' ', '_')
            download_filepath = f"variant_{safe_subject}.jpg"
        # Save image
        with open(download_filepath, "wb") as f:
            f.write(image_bytes)
        print(f"Saved generated image to {download_filepath}")
        return download_filepath
    finally:
        # Close file handles
        for f in image_files:
            f.close()
#Usage
# Auto-generate filename
output_file = generate_variant(
    image_filepaths=["M10263470_blue.jpg", "M10263470_blue_006.jpg"],
    email_subject="Premium Bluetooth Headphones",
    highlights=["Wireless", "30-hour battery", "Noise cancellation"],
    download_filepath="headphone_lead2.jpg"
)
print(f"Saved to: {output_file}")