# Based on the provided details, I'll update the etl_pipeline function to include the additional transformations
def etl_pipeline(file_path):
    import pandas as pd
    import numpy as np

    # Extract phase: Load the data from the given file path
    df = pd.read_csv(file_path)

    # Transform phase: Perform data cleaning and preprocessing
    # Drop rows where 'CustomerID' is NaN
    df = df.dropna(subset=['CustomerID'])
    
    # Fill NaN values in 'Discount_pct' with 'No Discount'
    df['Discount_pct'] = df['Discount_pct'].fillna('No Discount')
    
    # Drop unnecessary columns
    columns_to_delete = ["Unnamed: 0", "Transaction_Date", "Product_Description", "Coupon_Code"]
    df = df.drop(columns_to_delete, axis=1)

    # Rename columns
    df.rename(columns={
        "Tenure_Months": "Usage_Duration_Months",
        "Product_SKU": "Inventory_Number",
        "GST": "Tax",
        "Discount_pct": "Discount_%"
    }, inplace=True)

    # Data type conversions
    df['CustomerID'] = df['CustomerID'].astype('int').astype('string')
    df['Transaction_ID'] = df['Transaction_ID'].astype('int').astype('string')
    df['Tax'] = df['Tax'].astype('string')
    df['Month'] = df['Month'].astype('string')
    df['Quantity'] = df['Quantity'].astype('int')
    df['Date'] = pd.to_datetime(df['Date'])
    df['Gender'] = df['Gender'].astype('category')
    df['Coupon_Status'] = df['Coupon_Status'].astype('category')
    df['Discount_%'] = df['Discount_%'].astype('category')

    # Calculate sub_sum
    df['sub_sum'] = df['Quantity'] * df['Avg_Price']

    # Create Spend_Mode_Tendency
    df['Spend_Mode_Tendency'] = np.where(df['Online_Spend'] >= df['Offline_Spend'], 'Online', 'Offline')

    # Load phase: Since the task does not specify a destination, we return the transformed DataFrame
    return df

# Example of using the function:
# df_transformed = etl_pipeline('path_to_your_csv_file.csv')
