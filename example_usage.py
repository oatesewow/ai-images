#!/usr/bin/env python3
"""
Example usage of replace_position_zero_variant.py

This script demonstrates how to use the image replacement functionality.
"""

from replace_position_zero_variant import (
    process_image_replacement_with_redshift,
    process_batch_with_workers,
    load_image_ids_from_file
)
import sys


def batch_image_replacement_with_workers():
    """
    Multi-threaded batch processing with hardcoded image IDs
    """
    # List of image IDs to process
    image_ids = []
    
    if not image_ids:
        print("⚠️ No image IDs specified!")
        print("Edit the image_ids list in this function to add your image IDs.")
        return
    
    # Process with 25 workers (adjust as needed)
    result = process_batch_with_workers(
        image_ids=image_ids,
        max_workers=25,
        update_redshift=True
    )
    
    return result


def batch_from_file():
    """
    Process image IDs loaded from a file
    """
    print("Available file formats:")
    print("- CSV with 'image_id' column")
    print("- Excel with 'image_id' column") 
    print("- Text file with one image ID per line")
    print()
    
    file_path = input("Enter file path: ").strip()
    
    try:
        # Load image IDs from file
        image_ids = load_image_ids_from_file(file_path)
        
        if not image_ids:
            print("No valid image IDs found in file!")
            return
        
        # Ask for number of workers
        workers_input = input(f"Number of workers (default: 25): ").strip()
        max_workers = int(workers_input) if workers_input.isdigit() else 25
        
        # Process the batch
        result = process_batch_with_workers(
            image_ids=image_ids,
            max_workers=max_workers,
            update_redshift=True
        )
        
        return result
        
    except Exception as e:
        print(f"Error processing file: {e}")
        return None


if __name__ == "__main__":
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "batch-workers":
            batch_image_replacement_with_workers()
        elif command == "batch-file":
            batch_from_file()
        elif command.isdigit():
            # Process single image ID passed as argument
            image_id = int(sys.argv[1])
            print(f"Processing image ID from command line: {image_id}")
            result = process_image_replacement_with_redshift(image_id, update_redshift=True)
            if result['success']:
                print(f"✅ Successfully processed image {image_id}")
                if result.get('redshift_updated'):
                    print("✅ Redshift status updated")
                else:
                    print("⚠️ Redshift status not updated")
            else:
                print(f"❌ Failed to process image {image_id}: {result['error']}")
        else:
            print("Usage:")
            print("  python example_usage.py [image_id]     # Process single image")
            print("  python example_usage.py batch-workers  # Process batch (multi-threaded)")
            print("  python example_usage.py batch-file     # Process batch from file")
    else:
        print("Usage:")
        print("  python example_usage.py [image_id]     # Process single image")
        print("  python example_usage.py batch-workers  # Process batch (multi-threaded)")
        print("  python example_usage.py batch-file     # Process batch from file") 