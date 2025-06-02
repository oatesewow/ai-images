import streamlit as st
import pandas as pd
import os
import requests
from io import BytesIO
from PIL import Image
import numpy as np
import tempfile
import gc
import time
from functools import lru_cache

# Set page config
st.set_page_config(layout="wide", page_title="Image Variant Review Tool")

# Initialize session state for navigation and data
if 'item_index' not in st.session_state:
    st.session_state.item_index = 1
if 'reviewed_data' not in st.session_state:
    st.session_state.reviewed_data = None
if 'temp_file_path' not in st.session_state:
    st.session_state.temp_file_path = None
if 'current_df' not in st.session_state:
    st.session_state.current_df = None
if 'results_count' not in st.session_state:
    st.session_state.results_count = {'approved': 0, 'rejected': 0, 'regenerate': 0, 'pending': 0}
if 'preloaded_images' not in st.session_state:
    st.session_state.preloaded_images = {}
if 'next_item_preloaded' not in st.session_state:
    st.session_state.next_item_preloaded = False
if 'last_action_time' not in st.session_state:
    st.session_state.last_action_time = time.time()

# Number of images to preload
PRELOAD_COUNT = 5

# Function to load an image from URL with memory optimization
@st.cache_data(ttl=300, max_entries=50)
def load_image_from_url(url):
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            img = Image.open(BytesIO(response.content))
            # Reduce memory by converting to RGB if RGBA and resize if too large
            if img.mode == 'RGBA':
                img = img.convert('RGB')
            # Limit image size to reduce memory usage
            if max(img.size) > 1200:
                img.thumbnail((1200, 1200), Image.LANCZOS)
            return img
        else:
            st.error(f"Failed to load image: {response.status_code}")
            return None
    except Exception as e:
        st.error(f"Error loading image: {str(e)}")
        return None

# Version of load_image that doesn't use streamlit cache for prefetching
@lru_cache(maxsize=20)
def prefetch_image(url):
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            img = Image.open(BytesIO(response.content))
            if img.mode == 'RGBA':
                img = img.convert('RGB')
            if max(img.size) > 1200:
                img.thumbnail((1200, 1200), Image.LANCZOS)
            # Just return True to indicate success
            return True
        return False
    except:
        return False

# Preload images without using threading to avoid session state issues
def preload_images(df, filtered_indices, current_index, count=PRELOAD_COUNT):
    try:
        total = len(filtered_indices)
        preloaded = 0
        urls_to_preload = []
        
        # Collect URLs to preload
        for i in range(current_index, min(current_index + count, total)):
            if i >= total:
                break
                
            idx = filtered_indices[i]
            row = df.iloc[idx]
            
            # Check if we have the original image URL
            if 'image_url_pos_0' in row and row['image_url_pos_0']:
                url = row['image_url_pos_0']
                if url not in st.session_state.preloaded_images:
                    urls_to_preload.append(url)
            
            # Check if we have the variant image URL
            if 's3_url' in row and row['s3_url']:
                url = row['s3_url']
                if url not in st.session_state.preloaded_images:
                    urls_to_preload.append(url)
        
        # Preload the most imminent images first (next item)
        if current_index < total:
            next_idx = filtered_indices[current_index]
            next_row = df.iloc[next_idx]
            
            if 'image_url_pos_0' in next_row and next_row['image_url_pos_0']:
                url = next_row['image_url_pos_0']
                # Use the streamlit cache version for immediate next
                img = load_image_from_url(url)
                if img is not None:
                    st.session_state.preloaded_images[url] = "loaded"
                    preloaded += 1
            
            if 's3_url' in next_row and next_row['s3_url']:
                url = next_row['s3_url']
                # Use the streamlit cache version for immediate next
                img = load_image_from_url(url)
                if img is not None:
                    st.session_state.preloaded_images[url] = "loaded"
                    preloaded += 1
            
            # Mark that we've preloaded the next item
            st.session_state.next_item_preloaded = True
            
        # For the rest, use prefetch which won't block or cause session issues
        for url in urls_to_preload:
            if url not in st.session_state.preloaded_images:
                # Just mark as preloaded - we've initiated the fetch
                st.session_state.preloaded_images[url] = "prefetched"
                # Call the prefetch function - this doesn't return the image
                # but primes the cache
                prefetch_image(url)
                preloaded += 1
                    
        # Clean up old preloaded images to avoid memory bloat
        if len(st.session_state.preloaded_images) > 30:
            # Keep only the most recent
            keys_to_keep = list(st.session_state.preloaded_images.keys())[-20:]
            new_preloaded = {}
            for key in keys_to_keep:
                new_preloaded[key] = st.session_state.preloaded_images[key]
            st.session_state.preloaded_images = new_preloaded
            
        return preloaded
            
    except Exception as e:
        # Don't crash the app if preloading fails
        print(f"Error preloading images: {str(e)}")
        return 0

# Load the data with chunking for large files
@st.cache_data(ttl=60)
def load_data(file_path):
    # For very large files, consider reading in chunks
    if file_path.endswith('.csv'):
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            st.error(f"Error loading CSV: {str(e)}")
            return None
    return None

# Save data function with incremental update
def save_data(df, file_path=None, row_index=None, result=None, notes=None):
    # Update the results count
    if result:
        if result == 'approved':
            st.session_state.results_count['approved'] += 1
        elif result == 'rejected':
            st.session_state.results_count['rejected'] += 1
        elif result == 'regenerate':
            st.session_state.results_count['regenerate'] += 1
        
        # Update pending count
        total = len(df)
        reviewed = st.session_state.results_count['approved'] + st.session_state.results_count['rejected'] + st.session_state.results_count['regenerate']
        st.session_state.results_count['pending'] = total - reviewed
    
    # If using a local file, save directly to it
    if file_path:
        # If we're just updating a single row, only write that change
        if row_index is not None and (result or notes is not None):
            try:
                # Always do a full save to ensure consistency
                df.to_csv(file_path, index=False)
                if result:
                    st.success(f"Updated row {row_index} in {file_path}")
                elif notes is not None:
                    st.success(f"Notes saved for row {row_index} in {file_path}")
            except Exception as e:
                # If save fails, show error
                st.error(f"Error saving to {file_path}: {str(e)}")
        else:
            # Full save
            df.to_csv(file_path, index=False)
            st.success(f"Data saved to {file_path}")
    else:
        # If using an uploaded file, we need to save to a temporary file
        if st.session_state.temp_file_path is None:
            temp_dir = tempfile.mkdtemp()
            temp_file = os.path.join(temp_dir, "reviewed_data.csv")
            st.session_state.temp_file_path = temp_file
        
        # Save to the temporary file
        df.to_csv(st.session_state.temp_file_path, index=False)
        # Update the session state data
        st.session_state.current_df = df
        if result:
            st.success("Data saved")
        elif notes is not None:
            st.success("Notes saved")
    
    # Run garbage collection to free memory
    gc.collect()
    
    return df

# Navigation functions
def go_to_next(increment=1):
    st.session_state.item_index += increment
    st.session_state.next_item_preloaded = False  # Reset preload flag
    st.session_state.last_action_time = time.time()
    
def go_to_previous(decrement=1):
    st.session_state.item_index -= decrement
    st.session_state.next_item_preloaded = False  # Reset preload flag
    st.session_state.last_action_time = time.time()

# Clear image cache to prevent memory issues
def clear_image_cache():
    # Clear the cache for the load_image_from_url function
    load_image_from_url.clear()
    gc.collect()

# Handle review result
def handle_review(df, original_index, result, use_local_file, local_file_path, total_filtered):
    # Update dataframe
    df.loc[original_index, 'review_result'] = result
    
    # Save data
    if use_local_file:
        df = save_data(df, local_file_path, original_index, result)
    else:
        df = save_data(df, row_index=original_index, result=result)
    
    # Reset preload flag
    st.session_state.next_item_preloaded = False
    
    # Go to next item if not at the end
    if st.session_state.item_index < total_filtered:
        go_to_next()

# Main function
def main():
    st.title("Image Variant Review Tool")
    
    # File uploader for CSV
    uploaded_file = st.sidebar.file_uploader("Choose your winners CSV file", type="csv", key="file_uploader")
    
    # Option to use a local file path instead
    use_local_file = st.sidebar.checkbox("Use local file instead")
    local_file_path = st.sidebar.text_input("Local file path", value="New_Testing/top 500.csv") if use_local_file else None
    
    data_source = local_file_path if use_local_file and local_file_path else uploaded_file
    
    if data_source is not None:
        try:
            # Load the data
            if st.session_state.current_df is not None:
                # Use the data we already have in session state
                df = st.session_state.current_df
            else:
                # First time loading the data
                if use_local_file:
                    df = load_data(local_file_path)
                else:
                    df = pd.read_csv(uploaded_file)
                
                # Store in session state
                st.session_state.current_df = df
            
            # Create columns for feedback if they don't exist
            if 'review_result' not in df.columns:
                df['review_result'] = ""
            if 'review_notes' not in df.columns:
                df['review_notes'] = ""
            
            # Get total number of rows
            total_rows = len(df)
            
            # Update the initial results count if needed
            if st.session_state.results_count['pending'] == 0:
                approved = len(df[df['review_result'] == 'approved'])
                rejected = len(df[df['review_result'] == 'rejected'])
                regenerate = len(df[df['review_result'] == 'regenerate'])
                pending = total_rows - approved - rejected - regenerate
                
                st.session_state.results_count = {
                    'approved': approved,
                    'rejected': rejected,
                    'regenerate': regenerate,
                    'pending': pending
                }
            
            # Create a navigation system
            st.sidebar.write(f"Total items: {total_rows}")
            
            # Filters
            st.sidebar.header("Filters")
            
            # Filter by review status
            review_status = st.sidebar.selectbox(
                "Filter by review status",
                ["All", "Pending", "Approved", "Rejected", "Regenerate"]
            )
            
            if review_status == "Pending":
                df_filtered = df[df['review_result'].isna() | (df['review_result'] == "")]
            elif review_status != "All":
                df_filtered = df[df['review_result'] == review_status.lower()]
            else:
                df_filtered = df
                
            # Filter by category
            if 'category_name' in df.columns:
                categories = ["All"] + sorted(df['category_name'].unique().tolist())
                selected_category = st.sidebar.selectbox("Filter by category", categories)
                
                if selected_category != "All":
                    df_filtered = df_filtered[df_filtered['category_name'] == selected_category]
            
            # Navigation
            filtered_indices = df_filtered.index.tolist()
            total_filtered = len(filtered_indices)
            
            st.sidebar.write(f"Filtered items: {total_filtered}")
            
            if total_filtered > 0:
                # Ensure item_index is within valid range
                if st.session_state.item_index > total_filtered:
                    st.session_state.item_index = total_filtered
                if st.session_state.item_index < 1:
                    st.session_state.item_index = 1
                
                # Pagination
                item_index = st.sidebar.number_input(
                    "Item #", 
                    min_value=1, 
                    max_value=total_filtered, 
                    value=st.session_state.item_index,
                    key="item_number"
                )
                st.session_state.item_index = item_index
                
                # Get the actual index in the original dataframe
                original_index = filtered_indices[st.session_state.item_index - 1]
                row = df.iloc[original_index]
                
                # Display item info in the main area
                col1, col2, col3 = st.columns([1, 1, 1])
                with col1:
                    st.subheader("Item Details")
                    st.write(f"**ID:** {row['id']}")
                    st.write(f"**Product:** {row['email_subject']}")
                    if 'category_name' in row:
                        st.write(f"**Category:** {row['category_name']}")
                    if 'revenue_last_14_days' in row:
                        st.write(f"**Revenue (14 days):** £{row['revenue_last_14_days']}")
                
                # If not already preloaded, start preloading now
                if not st.session_state.next_item_preloaded and time.time() - st.session_state.last_action_time > 0.5:
                    # Don't preload immediately after an action to avoid session state issues
                    preload_count = preload_images(df, filtered_indices, st.session_state.item_index)
                
                # Create columns for images
                col1, col2 = st.columns(2)
                
                # Display the original image
                with col1:
                    st.subheader("Current Image")
                    if 'image_url_pos_0' in row and row['image_url_pos_0']:
                        current_url = row['image_url_pos_0']
                        is_preloaded = current_url in st.session_state.preloaded_images
                        
                        img = load_image_from_url(current_url)
                        if not is_preloaded:
                            st.session_state.preloaded_images[current_url] = "loaded"
                            
                        if img:
                            # Use smaller image to reduce memory
                            st.image(img, use_column_width=True)
                        else:
                            st.error("Could not load original image")
                    else:
                        st.error("No original image URL available")
                
                # Display the variant image
                with col2:
                    st.subheader("Variant Image")
                    if 's3_url' in row and row['s3_url']:
                        variant_url = row['s3_url']
                        is_preloaded = variant_url in st.session_state.preloaded_images
                        
                        img = load_image_from_url(variant_url)
                        if not is_preloaded:
                            st.session_state.preloaded_images[variant_url] = "loaded"
                            
                        if img:
                            # Use smaller image to reduce memory
                            st.image(img, use_column_width=True)
                        else:
                            st.error("Could not load variant image")
                    else:
                        st.error("No variant image URL available")
                
                # Review section
                st.subheader("Review")
                
                # Show current review status if available
                current_result = row['review_result'] if not pd.isna(row['review_result']) else ""
                st.write(f"Current status: **{current_result}**")
                
                # Create columns for buttons
                btn_col1, btn_col2, btn_col3, btn_col4 = st.columns(4)
                
                with btn_col1:
                    if st.button("Approve", key="approve", type="primary"):
                        # Use a function instead of inline code to improve transitions
                        handle_review(df, original_index, "approved", use_local_file, local_file_path, total_filtered)
                        st.rerun()
                
                with btn_col2:
                    if st.button("Reject", key="reject"):
                        handle_review(df, original_index, "rejected", use_local_file, local_file_path, total_filtered) 
                        st.rerun()
                
                with btn_col3:
                    if st.button("Regenerate", key="regenerate"):
                        handle_review(df, original_index, "regenerate", use_local_file, local_file_path, total_filtered)
                        st.rerun()
                
                # Notes area
                notes = st.text_area("Review notes", value=row['review_notes'] if not pd.isna(row['review_notes']) else "")
                if st.button("Save Notes"):
                    df.loc[original_index, 'review_notes'] = notes
                    # Update the session state dataframe
                    st.session_state.current_df = df
                    if use_local_file:
                        df = save_data(df, local_file_path, original_index, notes=notes)
                    else:
                        df = save_data(df, row_index=original_index, notes=notes)
                    st.success("Notes saved!")
                    st.rerun()
                
                # Navigation buttons
                nav_col1, nav_col2 = st.columns(2)
                with nav_col1:
                    if st.session_state.item_index > 1:
                        if st.button("Previous"):
                            go_to_previous()
                            st.rerun()
                
                with nav_col2:
                    if st.session_state.item_index < total_filtered:
                        if st.button("Next"):
                            go_to_next()
                            st.rerun()
                
                # Progress bar
                progress = st.session_state.item_index / total_filtered
                st.progress(progress)
                st.write(f"Reviewed: {st.session_state.item_index}/{total_filtered} ({int(progress*100)}%)")
                
                # Summary stats - Use session state counts for consistency
                st.sidebar.header("Review Summary")
                
                st.sidebar.write(f"Approved: {st.session_state.results_count['approved']}")
                st.sidebar.write(f"Rejected: {st.session_state.results_count['rejected']}")
                st.sidebar.write(f"Regenerate: {st.session_state.results_count['regenerate']}")
                st.sidebar.write(f"Pending: {st.session_state.results_count['pending']}")
                
                # Preloading status
                preloaded_count = len(st.session_state.preloaded_images)
                st.sidebar.divider()
                st.sidebar.markdown(f"### Image Preloading")
                st.sidebar.write(f"Preloaded images: {preloaded_count}")
                if st.sidebar.button("Force Preload"):
                    preloaded = preload_images(df, filtered_indices, st.session_state.item_index, count=10)
                    st.sidebar.success(f"Preloaded {preloaded} new images")
                
                # Download the updated CSV
                csv = df.to_csv(index=False).encode('utf-8')
                
                # Get appropriate file name
                if use_local_file:
                    file_name = os.path.basename(local_file_path)
                else:
                    file_name = "reviewed_data.csv" if uploaded_file is None else uploaded_file.name
                
                st.sidebar.download_button(
                    "Download Updated CSV",
                    csv,
                    f"updated_{file_name}",
                    "text/csv",
                    key='download-csv'
                )
                
                # Memory usage info
                st.sidebar.divider()
                st.sidebar.markdown("### Memory Usage")
                if st.sidebar.button("Clear Cache"):
                    st.cache_data.clear()
                    prefetch_image.cache_clear()
                    st.session_state.preloaded_images = {}
                    st.rerun()
                
            else:
                st.write("No items match the current filters.")
        
        except Exception as e:
            st.error(f"Error loading data: {str(e)}")
    
    else:
        st.write("Please upload a CSV file or specify a local file path to start reviewing.")

if __name__ == "__main__":
    main() 