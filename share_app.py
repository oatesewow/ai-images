import os
import subprocess
import time
from pyngrok import ngrok

# Start Streamlit in the background
streamlit_process = subprocess.Popen(
    ["streamlit", "run", "review_interface.py", "--server.port=8501"],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE
)

# Wait a moment for Streamlit to start
time.sleep(5)

try:
    # Open a ngrok tunnel to the Streamlit port
    public_url = ngrok.connect(8501).public_url
    print(f"Streamlit app is running!")
    print(f"Your public URL is: {public_url}")
    print("Press Ctrl+C to stop the app")
    
    # Keep the script running
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    # Close the ngrok tunnel
    ngrok.kill()
    # Terminate the Streamlit process
    streamlit_process.terminate()
    print("App stopped") 