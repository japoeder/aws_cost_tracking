import pandas as pd

def date_expand(df):
    # Ensure STARTDATE is a datetime type
    df['STARTDATE'] = pd.to_datetime(df[['YEAR', 'MONTH', 'DAY']])
    
    # Create a complete date range from min_date to max_date
    min_date = df['STARTDATE'].min()
    max_date = df['STARTDATE'].max()
    complete_date_range = pd.date_range(start=min_date, end=max_date, freq='D')
    
    # Create a DataFrame with the complete date range
    complete_df = pd.DataFrame({'STARTDATE': complete_date_range})
    
    # Merge the complete date range with the original data
    merged_df = pd.merge(complete_df, df, on='STARTDATE', how='left')
    
    # Fill missing values with 0 for USD
    merged_df['USD'] = merged_df['USD'].fillna(0)
    
    # Fill forward the SERVICE, YEAR, MONTH, DAY columns
    merged_df['SERVICE'] = merged_df['SERVICE'].ffill()
    merged_df['YEAR'] = merged_df['STARTDATE'].dt.year
    merged_df['MONTH'] = merged_df['STARTDATE'].dt.month
    merged_df['DAY'] = merged_df['STARTDATE'].dt.day
    
    return merged_df