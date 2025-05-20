import streamlit as st
import pandas as pd
import os
import requests
from io import BytesIO
from PIL import Image
import numpy as np
import tempfile

# Set page config
st.set_page_config(layout="wide", page_title="Image Variant Review Tool")

# Initialize session state for navigation and data
if 'item_index' not in st.session_state:
    st.session_state.item_index = 1
if 'reviewed_data' not in st.session_state:
    st.session_state.reviewed_data = None
if 'temp_file_path' not in st.session_state:
    st.session_state.temp_file_path = None

# Function to load an image from URL
@st.cache_data
def load_image_from_url(url):
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return Image.open(BytesIO(response.content))
        else:
            st.error(f"Failed to load image: {response.status_code}")
            return None
    except Exception as e:
        st.error(f"Error loading image: {str(e)}")
        return None

# Load the data
@st.cache_data
def load_data(file_path):
    return pd.read_csv(file_path)

# Save data function
def save_data(df, file_path=None):
    # If we're using a local file, save directly to it
    if file_path:
        df.to_csv(file_path, index=False)
        st.success(f"Data saved to {file_path}")
    else:
        # If using an uploaded file, we need to save to a temporary file
        # and update the session state
        if st.session_state.temp_file_path is None:
            temp_dir = tempfile.mkdtemp()
            temp_file = os.path.join(temp_dir, "reviewed_data.csv")
            st.session_state.temp_file_path = temp_file
        
        # Save to the temporary file
        df.to_csv(st.session_state.temp_file_path, index=False)
        # Update the session state data
        st.session_state.reviewed_data = df
        st.success("Data saved")
    
    return df

# Navigation functions
def go_to_next():
    st.session_state.item_index += 1
    
def go_to_previous():
    st.session_state.item_index -= 1

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
            if use_local_file:
                df = pd.read_csv(local_file_path)
            else:
                if st.session_state.reviewed_data is not None:
                    # Use the data we already have in session state
                    df = st.session_state.reviewed_data
                else:
                    # First time loading the data
                    df = pd.read_csv(uploaded_file)
            
            # Create columns for feedback if they don't exist
            if 'review_result' not in df.columns:
                df['review_result'] = ""
            if 'review_notes' not in df.columns:
                df['review_notes'] = ""
            
            # Get total number of rows
            total_rows = len(df)
            
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
                
                # Create columns for images
                col1, col2 = st.columns(2)
                
                # Display the original image
                with col1:
                    st.subheader("Current Image")
                    if 'image_url_pos_0' in row and row['image_url_pos_0']:
                        img = load_image_from_url(row['image_url_pos_0'])
                        if img:
                            # Ensure image displays in 3:2 aspect ratio
                            st.image(img, use_column_width=True)
                        else:
                            st.error("Could not load original image")
                    else:
                        st.error("No original image URL available")
                
                # Display the variant image
                with col2:
                    st.subheader("Variant Image")
                    if 's3_url' in row and row['s3_url']:
                        img = load_image_from_url(row['s3_url'])
                        if img:
                            # Ensure image displays in 3:2 aspect ratio
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
                        df.loc[original_index, 'review_result'] = "approved"
                        if use_local_file:
                            df = save_data(df, local_file_path)
                        else:
                            df = save_data(df)
                        # Go to next item if not at the end
                        if st.session_state.item_index < total_filtered:
                            go_to_next()
                        st.rerun()
                
                with btn_col2:
                    if st.button("Reject", key="reject"):
                        df.loc[original_index, 'review_result'] = "rejected"
                        if use_local_file:
                            df = save_data(df, local_file_path)
                        else:
                            df = save_data(df)
                        # Go to next item if not at the end
                        if st.session_state.item_index < total_filtered:
                            go_to_next()
                        st.rerun()
                
                with btn_col3:
                    if st.button("Regenerate", key="regenerate"):
                        df.loc[original_index, 'review_result'] = "regenerate"
                        if use_local_file:
                            df = save_data(df, local_file_path)
                        else:
                            df = save_data(df)
                        # Go to next item if not at the end
                        if st.session_state.item_index < total_filtered:
                            go_to_next()
                        st.rerun()
                
                # Notes area
                notes = st.text_area("Review notes", value=row['review_notes'] if not pd.isna(row['review_notes']) else "")
                if st.button("Save Notes"):
                    df.loc[original_index, 'review_notes'] = notes
                    if use_local_file:
                        df = save_data(df, local_file_path)
                    else:
                        df = save_data(df)
                    st.success("Notes saved!")
                
                # Navigation buttons
                nav_col1, nav_col2 = st.columns(2)
                with nav_col1:
                    if st.session_state.item_index > 1:
                        if st.button("Previous", on_click=go_to_previous):
                            pass
                
                with nav_col2:
                    if st.session_state.item_index < total_filtered:
                        if st.button("Next", on_click=go_to_next):
                            pass
                
                # Progress bar
                progress = st.session_state.item_index / total_filtered
                st.progress(progress)
                st.write(f"Reviewed: {st.session_state.item_index}/{total_filtered} ({int(progress*100)}%)")
                
                # Summary stats
                st.sidebar.header("Review Summary")
                approved = len(df[df['review_result'] == 'approved'])
                rejected = len(df[df['review_result'] == 'rejected'])
                regenerate = len(df[df['review_result'] == 'regenerate'])
                pending = total_rows - approved - rejected - regenerate
                
                st.sidebar.write(f"Approved: {approved}")
                st.sidebar.write(f"Rejected: {rejected}")
                st.sidebar.write(f"Regenerate: {regenerate}")
                st.sidebar.write(f"Pending: {pending}")
                
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
                
            else:
                st.write("No items match the current filters.")
        
        except Exception as e:
            st.error(f"Error loading data: {str(e)}")
    
    else:
        st.write("Please upload a CSV file or specify a local file path to start reviewing.")

if __name__ == "__main__":
    main() 