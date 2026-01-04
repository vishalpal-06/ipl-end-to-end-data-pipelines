import pandas as pd
import os
import time  # <-- Added for delay

# Define output root directory
output_root = 'row'

# Create required directories
os.makedirs(os.path.join(output_root, 'delivery'), exist_ok=True)
os.makedirs(os.path.join(output_root, 'match'), exist_ok=True)

# Read matches and deliveries
df_matches = pd.read_csv('data/matches.csv')
df_deliveries = pd.read_csv('data/deliveries.csv')

# Convert date for proper ordering
if 'date' in df_matches.columns:
    df_matches['date'] = pd.to_datetime(df_matches['date'], dayfirst=True, errors='coerce')

# Get already processed match_ids
match_output_dir = os.path.join(output_root, 'match')
processed_matches = set()

if os.path.exists(match_output_dir):
    for filename in os.listdir(match_output_dir):
        if filename.endswith('.csv'):
            try:
                match_id = int(filename.replace('.csv', ''))
                processed_matches.add(match_id)
            except ValueError:
                pass

# Filter new matches
df_matches_to_process = df_matches[~df_matches['id'].isin(processed_matches)]

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
        delivery_dir = os.path.join(output_root, 'delivery', str(match_id))
        os.makedirs(delivery_dir, exist_ok=True)
        
        for inning in sorted(df_match_deliveries['inning'].unique()):
            df_inning = df_match_deliveries[df_match_deliveries['inning'] == inning]
            for over in sorted(df_inning['over'].unique()):
                df_over = df_inning[df_inning['over'] == over]
                if 'ball' in df_over.columns:
                    df_over = df_over.sort_values('ball')
                
                file_name = f"inning_{inning}_over_{over:02d}.csv"
                file_path = os.path.join(delivery_dir, file_name)
                df_over.to_csv(file_path, index=False)
    
    # === Save single match record ===
    match_file_path = os.path.join(output_root, 'match', f"{match_id}.csv")
    pd.DataFrame([match_row]).to_csv(match_file_path, index=False)
    
    processed_count += 1
    print(f"Completed match_id: {match_id} | Taking 1-minute break...\n")
    
    # 1-minute pause after each match
    time.sleep(10)

# Final message
if processed_count > 0:
    print(f"Done! Processed {processed_count} new match(es).")
else:
    print("No new matches to process.")