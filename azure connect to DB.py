from azure.storage.blob import BlobServiceClient
import pymysql

# Blob storage account details
STORAGE_CONNECTION_STRING = ""  # Set your Azure Blob Storage connection string
BLOB_CONTAINER_NAME = ""

# MySQL database connection details
DB_HOST = "127.0.0.1"
DB_USER = "root"
DB_PASSWORD = "root"
DB_NAME = "sys"

# Initialize BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)

# Connect to the database
db_connection = pymysql.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME
)
cursor = db_connection.cursor()

# Prefix for filtering blobs
prefix = ""

# Function to format the file name in the desired style (e.g., "0-2-0.nl.html")
def format_filename(blob_name):
    # Assuming we simply want the file name without any folder prefixes
    filename = blob_name.split("/")[-1]  # Extract the actual file name from the path
    return filename

# List and read only blobs with the specified prefix
for blob in container_client.list_blobs(name_starts_with=prefix):
    blob_name = blob.name
    blob_client = container_client.get_blob_client(blob_name)
    
    print(f'Reading file: {blob_name}...')
    
    # Download the blob's content
    blob_data = blob_client.download_blob()
    html_content = blob_data.readall().decode('utf-8')  # Ensure you're reading the HTML content correctly
    
    # Format the HTML content and file name
    formatted_filename = format_filename(blob_name)
    formatted_html = f"{html_content}\n<!-- Inserted content from file: {formatted_filename} -->"
    
    # Debug: Print a portion of the formatted HTML content to verify it's correct
    print(f"Formatted HTML Content Preview: {formatted_html[:200]}")  # Preview first 200 characters
    
    # Insert the formatted HTML content into the database
    insert_query = "INSERT INTO azure_database (azure_domain) VALUES (%s)"
    cursor.execute(insert_query, (formatted_html,))
    db_connection.commit()

    print(f'Inserted content from file: {formatted_filename}')

# Close the database connection
cursor.close()
db_connection.close()
