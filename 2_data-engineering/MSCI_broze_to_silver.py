import pandas as pd
import os

# Define the path to the CSV file
data_folder = os.path.join(os.path.dirname(__file__), '..', '0_data-bronze')
csv_file = os.path.join(data_folder, 'downloaded_MSCI.csv')

# Load the CSV file into a pandas DataFrame
df = pd.read_csv(csv_file)

# Ensure the date column is in datetime format
df['Date'] = pd.to_datetime(df['Date'])

# Set the date column as the index
df.set_index('Date', inplace=True)

# Resample the data to get the last closing price of each week (last trading day of each week)
weekly_df = df['Price'].resample('W-FRI').last()

# Resample the original daily data to get the last closing price of each day
daily_df = df['Price'].resample('D').last()

# Create a new DataFrame to store the required columns
result_df = pd.DataFrame()
result_df['Date'] = daily_df.index
result_df['MSCI Price'] = daily_df.values

# Shift the 'MSCI Price' column to get the 'Previous Day's MSCI Price'
result_df['Previous Day MSCI Price'] = result_df['MSCI Price'].shift(1)

# Calculate the MSCI Change from the previous day's closing
result_df['MSCI Change'] = result_df['MSCI Price'] - result_df['Previous Day MSCI Price']

# Calculate the percentage change from the previous day's closing
result_df['% MSCI Change'] = ((result_df['MSCI Price'] - result_df['Previous Day MSCI Price']) / result_df['Previous Day MSCI Price']) * 100

# Round up % MSCI Change to 2 decimal places
result_df['% MSCI Change'] = result_df['% MSCI Change'].apply(lambda x: round(x, 2))

# Keep only relevant Columns
result_df = result_df[['Date', '% MSCI Change']]

# Drop the first row since it won't have a 'Previous Day MSCI Price'
result_df.dropna(inplace=True)

# Reset the index to have a clean DataFrame
result_df.reset_index(drop=True, inplace=True)

# Define the path to save the new CSV file in the "silver" folder
silver_folder = os.path.join(os.path.dirname(__file__), '..', '0-data-silver')
output_file = os.path.join(silver_folder, 'MSCI.csv')

# Save the new DataFrame to a CSV file in the "silver" folder
result_df.to_csv(output_file, index=False)

# Display the new DataFrame
print(result_df)