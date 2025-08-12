# Image Variant Review Tool

A Streamlit application for reviewing image variants and providing feedback.

## Features

- Upload a CSV file with image URLs or use a local file
- Review original images alongside generated variants
- Approve, reject, or request regeneration of image variants
- Add review notes for each image
- Filter by review status and categories
- Track review progress
- Download updated CSV with review results

## Local Development

### Requirements

- Python 3.9+
- Required packages listed in `requirements.txt`

### Installation

1. Clone this repository:
```bash
git clone https://github.com/yourusername/streamlit-image-review.git
cd streamlit-image-review
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

### Running the App Locally

```bash
streamlit run review_interface.py
```

The app will be available at http://localhost:8501

## Data Format

The application expects a CSV file with the following columns:
- `id`: Product ID
- `email_subject`: Product name/description
- `image_url_pos_0`: URL to the original product image
- `s3_url`: URL to the generated variant image
- Optional: `category_name`, `revenue_last_14_days`

Review results will be saved to the CSV with additional columns:
- `review_result`: Approved/Rejected/Regenerate
- `review_notes`: Additional feedback

## Deployment

### Streamlit Cloud

Use the minimal dependencies in `requirements.txt` (Streamlit + data libs only) and deploy `review_interface.py`.

### Flask Service

Run the Flask app locally or on a server with the extra dependencies in `requirements-flask.txt`:

```bash
pip install -r requirements-flask.txt
python -m flask_app.app
```

For Streamlit Cloud specifics, see [Streamlit Cloud Deployment Guide](streamlit_cloud_deploy_instructions.md).

## License

This project is licensed under the MIT License - see the LICENSE file for details. 