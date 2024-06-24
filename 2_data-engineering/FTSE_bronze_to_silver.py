import pandas as pd
import os

# Define the path to the CSV file
data_folder = os.path.join(os.path.dirname(__file__), '..', '0_data-bronze')
csv_file = os.path.join(data_folder, 'downloaded_FTSE100.csv')

# Load the CSV file into a pandas DataFrame
df = pd.read_csv(csv_file)

# Ensure the date column is in datetime format (assuming the column is named 'date')
df['date'] = pd.to_datetime(df['date'])

# Set the date column as the index
df.set_index('date', inplace=True)

# Resample the data to get the last closing price of each week (last trading day of each week)
weekly_df = df['Price'].resample('W-FRI').last()

# Resample the original daily data to get the last closing price of each day
daily_df = df['Price'].resample('D').last()

# Create a new DataFrame to store the required columns
result_df = pd.DataFrame()
result_df['Date'] = daily_df.index
result_df['FTSE100 Price'] = daily_df.values

# Shift the 'FTSE100 Price' column to get the 'Previous Day's FTSE100 Price'
result_df['Previous Day FTSE100 Price'] = result_df['FTSE100 Price'].shift(1)

# Calculate the FTSE100 Change from the previous day's closing
result_df['FTSE100 Change'] = result_df['FTSE100 Price'] - result_df['Previous Day FTSE100 Price']

# Calculate the percentage change from the previous day's closing
result_df['% FTSE100 Change'] = ((result_df['FTSE100 Price'] - result_df['Previous Day FTSE100 Price']) / result_df['Previous Day FTSE100 Price']) * 100

# Round up % FTSE100 Change to 2 decimal places
result_df['% FTSE100 Change'] = result_df['% FTSE100 Change'].apply(lambda x: round(x, 2))

# Shift the '% FTSE100 Change' column to get the 'Previous Day % FTSE100 Change'
result_df['Previous Day % FTSE100 Change'] = result_df['% FTSE100 Change'].shift(1)

# Keep only relevant Columns
result_df = result_df[['Date', '% FTSE100 Change', 'Previous Day % FTSE100 Change']].dropna()

# Reset the index to have a clean DataFrame
result_df.reset_index(drop=True, inplace=True)

# Define the path to save the new CSV file in the "silver" folder
silver_folder = os.path.join(os.path.dirname(__file__), '..', '0-data-silver')
output_file = os.path.join(silver_folder, 'FTSE100.csv')

# Save the new DataFrame to a CSV file in the "silver" folder
result_df.to_csv(output_file, index=False)

# Display the new DataFrame
print(result_df)