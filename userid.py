import os
import requests
import json
import oracledb
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Environment variables
HUBSPOT_API_TOKEN = os.getenv("HUBSPOT_API_TOKEN")
PROD_DB_USER = os.getenv("ORACLE_USER")
PROD_DB_PASSWORD = os.getenv("ORACLE_PASSWORD")
PROD_DB_DSN = os.getenv("ORACLE_DSN")


def get_wowcher_user_id_from_deal_id(deal_id):
    """
    Complete pipeline: Deal ID -> HubSpot Owner Email -> Wowcher User ID
    """
    try:
        # Step 1: Get owner email from HubSpot
        deal_url = f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}?properties=hubspot_owner_id"
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {HUBSPOT_API_TOKEN}',
        }
        deal_response = requests.get(deal_url, headers=headers)
        if deal_response.status_code != 200:
            raise Exception(f"Failed to fetch deal data: {deal_response.text}")
        
        deal_data = deal_response.json()
        owner_id = deal_data.get("properties", {}).get("hubspot_owner_id")
        if not owner_id:
            raise Exception("Owner ID not found in deal data.")

        # Get email for HubSpot owner ID
        owner_url = f"https://api.hubapi.com/crm/v3/owners/{owner_id}?idProperty=id&archived=false"
        owner_response = requests.get(owner_url, headers=headers)
        if owner_response.status_code != 200:
            raise Exception(f"Failed to fetch owner data: {owner_response.text}")
        
        owner_data = owner_response.json()
        email = owner_data.get("email")
        if not email:
            raise Exception("Email not found for owner ID.")
        
        # Step 2: Get Wowcher user ID from Oracle
        connection = oracledb.connect(
            user=PROD_DB_USER,
            password=PROD_DB_PASSWORD,
            dsn=PROD_DB_DSN
        )
        
        # SQL query
        query = """
        SELECT u.id AS deal_owner_wowcher_user_id, email, FIRSTNAME, LASTNAME
        FROM users u
        JOIN user_role ur ON ur.USER_ID = u.ID
        JOIN role r ON r.ID = ur.ROLE_ID AND r.id = 210981 -- role_id = they must have salesperson role
        WHERE email = :email
        """
        
        # Execute query and get the user ID
        cursor = connection.cursor()
        cursor.execute(query, {'email': email})
        result = cursor.fetchone()
        
        # Close connection
        cursor.close()
        connection.close()
        
        if result:
            return result[0]  # Return just the user ID (first column)
        else:
            print(f"No Wowcher user found for email: {email}")
            return None
        
    except Exception as e:
        print(f"Error in pipeline: {e}")
        return None

def get_wowcher_user_id_from_owner_id(owner_id):
    """
    Pipeline: HubSpot Owner ID -> Email -> Wowcher User ID
    """
    try:
        # Step 1: Get email for HubSpot owner ID
        owner_url = f"https://api.hubapi.com/crm/v3/owners/{owner_id}?idProperty=id&archived=false"
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {HUBSPOT_API_TOKEN}',
        }
        owner_response = requests.get(owner_url, headers=headers)
        if owner_response.status_code != 200:
            raise Exception(f"Failed to fetch owner data: {owner_response.text}")
        
        owner_data = owner_response.json()
        email = owner_data.get("email")
        if not email:
            raise Exception("Email not found for owner ID.")
        
        # Step 2: Get Wowcher user ID from Oracle
        connection = oracledb.connect(
            user=PROD_DB_USER,
            password=PROD_DB_PASSWORD,
            dsn=PROD_DB_DSN
        )
        
        # SQL query
        query = """
        SELECT u.id AS deal_owner_wowcher_user_id, email, FIRSTNAME, LASTNAME
        FROM users u
        JOIN user_role ur ON ur.USER_ID = u.ID
        JOIN role r ON r.ID = ur.ROLE_ID AND r.id = 210981 -- role_id = they must have salesperson role
        WHERE email = :email
        """
        
        # Execute query and get the user ID
        cursor = connection.cursor()
        cursor.execute(query, {'email': email})
        result = cursor.fetchone()
        
        # Close connection
        cursor.close()
        connection.close()
        
        if result:
            print(f"Wowcher user found for email: {email}")
            return result[0]  # Return just the user ID (first column)
        else:
            print(f"No Wowcher user found for email: {email}")
            return None
        
    except Exception as e:
        print(f"Error in pipeline: {e}")
        return None

if __name__ == "__main__":
    try:
        # # Example 1: Starting with deal ID
        # deal_id = 263490931940  # Example hubspot ID
        # wowcher_user_id = get_wowcher_user_id_from_deal_id(deal_id)
        # if wowcher_user_id is not None:
        #     print(f"From Deal ID: Wowcher User ID = {wowcher_user_id}")
        # else:
        #     print("No user found from deal ID")
            
        # Example 2: Starting with owner ID
        owner_id = 76250190  # Example owner ID (you'll need to replace with actual ID)
        wowcher_user_id_2 = get_wowcher_user_id_from_owner_id(owner_id)
        if wowcher_user_id_2 is not None:
            print(f"From Owner ID: Wowcher User ID = {wowcher_user_id_2}")
        else:
            print("No user found from owner ID")
            
    except Exception as e:
        print("Error:", str(e))






