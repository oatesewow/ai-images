import oracledb
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_new_oracle_highlight_id():
    """
    Get a new highlight ID from Oracle sequence
    """
    connection = oracledb.connect(
        user=os.getenv("ORACLE_USER"),
        password=os.getenv("ORACLE_PASSWORD"),
        dsn=os.getenv("ORACLE_DSN")
    )
    cursor = connection.cursor()
    
    # Note: You may need to adjust the sequence name based on your database schema
    # Common patterns: deal_voucher_highlight_seq, highlight_seq, product_highlight_seq
    # If none of these work, check your Oracle database for the correct sequence name
    try:
        cursor.execute("SELECT deal_voucher_highlight_seq.NEXTVAL FROM dual")
    except Exception as e:
        # Try alternative sequence names if the first one fails
        try:
            cursor.execute("SELECT highlight_seq.NEXTVAL FROM dual")
        except:
            try:
                cursor.execute("SELECT product_highlight_seq.NEXTVAL FROM dual")
            except:
                # If no sequence is found, you may need to create one or use a different approach
                raise Exception(f"Could not find a suitable sequence for highlights. Original error: {e}")
    
    new_highlight_id = cursor.fetchone()[0]
    connection.commit()
    cursor.close()
    connection.close()
    return new_highlight_id

def insert_deal_highlight(deal_voucher_id, highlight_text):
    """
    Insert a new highlight into the Oracle database with proper ID generation
    
    Args:
        deal_voucher_id: The deal voucher ID
        highlight_text: The highlight text to insert
        
    Returns:
        new_highlight_id if successful, None if failed
    """
    try:
        # Get new highlight ID
        new_highlight_id = get_new_oracle_highlight_id()
        
        # Connect to Oracle
        connection = oracledb.connect(
            user=os.getenv("ORACLE_USER"),
            password=os.getenv("ORACLE_PASSWORD"),
            dsn=os.getenv("ORACLE_DSN")
        )
        cursor = connection.cursor()
        
        # Insert with proper ID - this is the corrected INSERT statement
        cursor.execute("""
            INSERT INTO deal_voucher_highlight (ID, deal_voucher_id, highlight) 
            VALUES (:highlight_id, :deal_voucher_id, :highlight)
        """, {
            "highlight_id": new_highlight_id,
            "deal_voucher_id": deal_voucher_id,
            "highlight": highlight_text
        })
        
        connection.commit()
        cursor.close()
        connection.close()
        
        print(f"✅ Successfully inserted highlight with ID {new_highlight_id}")
        return new_highlight_id
        
    except Exception as e:
        print(f"❌ Error inserting highlight: {str(e)}")
        return None

def insert_deal_highlight_with_manual_id(deal_voucher_id, highlight_text, highlight_id):
    """
    Insert a new highlight into the Oracle database with a manually specified ID
    Use this if you want to specify the ID yourself instead of using a sequence
    
    Args:
        deal_voucher_id: The deal voucher ID
        highlight_text: The highlight text to insert
        highlight_id: The ID to use for this highlight
        
    Returns:
        highlight_id if successful, None if failed
    """
    try:
        # Connect to Oracle
        connection = oracledb.connect(
            user=os.getenv("ORACLE_USER"),
            password=os.getenv("ORACLE_PASSWORD"),
            dsn=os.getenv("ORACLE_DSN")
        )
        cursor = connection.cursor()
        
        # Insert with manually specified ID
        cursor.execute("""
            INSERT INTO deal_voucher_highlight (ID, deal_voucher_id, highlight) 
            VALUES (:highlight_id, :deal_voucher_id, :highlight)
        """, {
            "highlight_id": highlight_id,
            "deal_voucher_id": deal_voucher_id,
            "highlight": highlight_text
        })
        
        connection.commit()
        cursor.close()
        connection.close()
        
        print(f"✅ Successfully inserted highlight with ID {highlight_id}")
        return highlight_id
        
    except Exception as e:
        print(f"❌ Error inserting highlight: {str(e)}")
        return None

# Example usage:
if __name__ == "__main__":
    # Example 1: Using automatic ID generation
    result1 = insert_deal_highlight(39923647, '<b>Summer Holidays:</b> Summer is for creating memories. Let\'s make this summer unforgettable!')
    
    # Example 2: Using manual ID specification (if you need to specify your own ID)
    result2 = insert_deal_highlight_with_manual_id(39923647, 'Another highlight text', 12345)
    
    print(f"Auto-generated ID result: {result1}")
    print(f"Manual ID result: {result2}") 