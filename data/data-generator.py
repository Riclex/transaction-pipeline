import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def generate_synthetic_transactions(num_records=10000, start_date="2025-01-01", end_date="2025-12-31"):
    """
    Generate synthetic transaction data
    """
    # Set random seed for reproducibility
    np.random.seed(42)
    random.seed(42)
    
    # Generate dates
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    date_range = (end_dt - start_dt).days
    
    # Define possible values
    currency = ['EUR']
    transaction_types = ['CARD', 'TRANSFER', 'REFUND', 'DEPOSIT', 'WITHDRAWAL', 'PAYMENT', 'FEE']
    statuses = ['COMPLETED', 'SETTLED', 'PENDING', 'FAILED', 'CANCELLED', 'OK']
    
    # Generate account IDs (more realistic distribution)
    num_accounts = max(1000, num_records // 20)  # Adjust based on number of records
    account_ids = [f'acc_{str(i).zfill(3)}' for i in range(1, num_accounts + 1)]
    
    # Generate transaction IDs
    txn_ids = [f'txn_{str(i).zfill(6)}' for i in range(1, num_records + 1)]
    
    data = []
    
    for i in range(num_records):
        # Random transaction date
        txn_date = start_dt + timedelta(days=random.randint(0, date_range))
        
        # Random account (with some accounts being more active)
        account_id = random.choice(account_ids)
        
        # Generate amount with realistic distribution (more small transactions)
        amount_type = random.random()
        if amount_type < 0.6:  # 60% small transactions
            amount = round(random.uniform(1, 100), 2)
        elif amount_type < 0.9:  # 30% medium transactions
            amount = round(random.uniform(100, 1000), 2)
        else:  # 10% large transactions
            amount = round(random.uniform(1000, 10000), 2)
        
        # Make some amounts negative based on transaction type
        txn_type = random.choice(transaction_types)
        if txn_type in ['REFUND', 'WITHDRAWAL', 'FEE']:
            amount = -abs(amount)
        elif txn_type == 'TRANSFER':
            # Transfers can be positive or negative
            amount = amount * random.choice([1, -1])
        
        # Currency (with EUR being most common)
        #currency = random.choices(currencies, weights=[40, 30, 15, 8, 5, 2], k=1)[0]
        
        # Status (most transactions completed)
        status = random.choices(statuses, weights=[50, 25, 10, 5, 5, 5], k=1)[0]
        
        # Ingestion date (usually same day, sometimes next day, rarely later)
        ingestion_delay = random.choices([0, 1, 2, 3, 7], weights=[70, 20, 5, 3, 2], k=1)[0]
        ingestion_date = txn_date + timedelta(days=ingestion_delay)
        
        data.append({
            'txn_id': txn_ids[i],
            'account_id': account_id,
            'txn_date': txn_date.strftime('%Y-%m-%d'),
            'ingestion_date': ingestion_date.strftime('%Y-%m-%d'),
            'amount': amount,
            'currency': currency,
            'txn_type': txn_type,
            'status': status
        })
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Sort by transaction date for more realistic ordering
    df = df.sort_values('txn_date').reset_index(drop=True)
    
    return df

def save_to_csv(df, filename='synthetic_transactions.csv'):
    """Save DataFrame to CSV file"""
    df.to_csv(filename, index=False)
    print(f"Generated {len(df)} records saved to {filename}")
    
    # Print some statistics
    print("\nDataset Statistics:")
    print(f"Total records: {len(df)}")
    print(f"Date range: {df['txn_date'].min()} to {df['txn_date'].max()}")
    print(f"Unique accounts: {df['account_id'].nunique()}")
    print(f"Total amount: {df['amount'].sum():.2f}")
    print(f"Currency distribution:")
    print(df['currency'].value_counts())
    print(f"\nTransaction type distribution:")
    print(df['txn_type'].value_counts())

# Generate and save data
if __name__ == "__main__":
    # Generate 10000 transactions
    df = generate_synthetic_transactions(num_records=10000)
    
    # Save to CSV
    save_to_csv(df, 'transactions_raw.csv')
    
    # Display first 10 rows
    print("\nFirst 10 rows of generated data:")
    print(df.head(10).to_string())