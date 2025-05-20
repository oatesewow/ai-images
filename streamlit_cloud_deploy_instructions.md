# Deploying to Streamlit Cloud

Follow these steps to deploy your Streamlit app to the cloud for sharing:

## 1. Create a GitHub Repository

1. Create a new repository on GitHub
2. Initialize git in your local project and push your code:

```bash
git init
git add .
git commit -m "Initial commit"
git branch -M main
git remote add origin <your-github-repo-url>
git push -u origin main
```

## 2. Sign up for Streamlit Cloud

1. Go to [Streamlit Cloud](https://streamlit.io/cloud)
2. Sign up for a free account using your GitHub credentials

## 3. Deploy the App

1. In Streamlit Cloud, click "New app"
2. Connect to the GitHub repository you created
3. In the app settings:
   - Set the main file path to: `review_interface.py`
   - Set Python version to 3.9 or later
   - Configure your advanced settings if needed (memory, secrets, etc.)

4. Click "Deploy"

Your app will be live at a URL like: `https://your-app-name.streamlit.app`

## 4. Managing Secrets (if needed)

If your app uses environment variables or secrets, add them in the Streamlit Cloud dashboard:

1. Go to your app settings
2. Click "Advanced settings" 
3. Under "Secrets", add your environment variables in TOML format:

```toml
[api_keys]
OPEN_AI_API_KEY = "your-openai-api-key"

[database]
REDSHIFT_HOST = "your-redshift-host"
REDSHIFT_PORT = "your-redshift-port"
REDSHIFT_DBNAME = "your-redshift-dbname"
REDSHIFT_USER = "your-redshift-username"
REDSHIFT_PASSWORD = "your-redshift-password"
```

## 5. Sharing Your App

- Once deployed, you'll get a public URL that you can share with anyone
- No additional setup is required for people to use your app
- They can access it directly through the browser without installing anything 