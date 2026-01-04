import pandas as pd
import io
import time
import os
from azure.storage.blob import BlobServiceClient

# ================= CONFIGURATION =================
CONNECTION_STRING = ""
CONTAINER_NAME = "ipl-incremental-data"
INPUT_DIR = "data"
# =================================================

def get_blob_service_client():
    return BlobServiceClient.from_connection_string(CONNECTION_STRING)

def upload_df_to_blob(container_client, df, blob_path):
    """
    Converts a DataFrame to a CSV string and uploads it to Azure Blob Storage.
    """
    output = io.StringIO()
    df.to_csv(output, index=False)
    csv_data = output.getvalue()
    
    blob_client = container_client.get_blob_client(blob_path)
    blob_client.upload_blob(csv_data, overwrite=True)

def get_processed_match_ids(container_client):
    """
    Lists blobs in 'row/match/' to find IDs that are already done.
    """
    processed_ids = set()
    # List all blobs that start with row/match/
    # This replaces os.listdir()
    blob_list = container_client.list_blobs(name_starts_with="row/match/")
    
    for blob in blob_list:
        # Blob name looks like: row/match/123.csv
        try:
            filename = blob.name.split('/')[-1] # Get '123.csv'
            match_id = int(filename.replace('.csv', ''))
            processed_ids.add(match_id)
        except ValueError:
            pass
    return processed_ids

def main():
    # Initialize Azure Client
    try:
        blob_service_client = get_blob_service_client()
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)
        # Check if container exists
        if not container_client.exists():
            print(f"Container '{CONTAINER_NAME}' does not exist. Please create it first.")
            return
    except Exception as e:
        print(f"Error connecting to Azure: {e}")
        return

    # Read local input data
    print("Reading local data...")
    try:
        df_matches = pd.read_csv(os.path.join(INPUT_DIR, 'matches.csv'))
        df_deliveries = pd.read_csv(os.path.join(INPUT_DIR, 'deliveries.csv'))
    except FileNotFoundError:
        print(f"Error: Could not find matches.csv or deliveries.csv in '{INPUT_DIR}' folder.")
        return

    # Convert date for proper ordering
    if 'date' in df_matches.columns:
        df_matches['date'] = pd.to_datetime(df_matches['date'], dayfirst=True, errors='coerce')

    # Get already processed match_ids from Azure
    print("Checking Azure for processed matches...")
    processed_matches = get_processed_match_ids(container_client)
    print(f"Found {len(processed_matches)} matches already processed.")

    # Filter new matches
    df_matches_to_process = df_matches[~df_matches['id'].isin(processed_matches)]

    # Sort by Date (as requested)
    if 'date' in df_matches.columns and df_matches['date'].notna().any():
        df_matches_to_process = df_matches_to_process.sort_values('date')
    else:
        df_matches_to_process = df_matches_to_process.sort_values('id')

    # Process each new match
    processed_count = 0

    for _, match_row in df_matches_to_process.iterrows():
        match_id = match_row['id']
        
        print(f"Processing match_id: {match_id} ...")

        # === Process Deliveries: over-by-over ===
        df_match_deliveries = df_deliveries[df_deliveries['match_id'] == match_id]
        
        if not df_match_deliveries.empty:
            for inning in sorted(df_match_deliveries['inning'].unique()):
                df_inning = df_match_deliveries[df_match_deliveries['inning'] == inning]
                for over in sorted(df_inning['over'].unique()):
                    df_over = df_inning[df_inning['over'] == over]
                    if 'ball' in df_over.columns:
                        df_over = df_over.sort_values('ball')
                    
                    # Define Blob Path
                    # Format: row/delivery/{match_id}/inning_{inning}_over_{over}.csv
                    file_name = f"inning_{inning}_over_{over:02d}.csv"
                    blob_path = f"row/delivery/{match_id}/{file_name}"
                    
                    # Upload to Azure
                    upload_df_to_blob(container_client, df_over, blob_path)
        
        # === Save single match record ===
        match_blob_path = f"row/match/{match_id}.csv"
        # Create a single-row DataFrame and upload
        upload_df_to_blob(container_client, pd.DataFrame([match_row]), match_blob_path)
        
        processed_count += 1
        print(f"Uploaded match_id: {match_id} to Azure | Pausing 10s...\n")
        
        # Pause (Logic from your previous code)
        time.sleep(10)

    # Final message
    if processed_count > 0:
        print(f"Done! Processed {processed_count} new match(es).")
    else:
        print("No new matches to process.")

if __name__ == "__main__":
    main()