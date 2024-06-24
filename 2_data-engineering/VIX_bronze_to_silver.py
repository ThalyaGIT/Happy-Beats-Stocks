import pandas as pd
import os

# Define the path to the CSV file
data_folder = os.path.join(os.path.dirname(__file__), '..', '0_data-bronze')
csv_file = os.path.join(data_folder, 'downloaded_VIX.csv')

# Load the CSV file into a pandas DataFrame
df = pd.read_csv(csv_file)

# Ensure the date column is in datetime format
df['Date'] = pd.to_datetime(df['Date'])

# Set the date column as the index
df.set_index('Date', inplace=True)

# Resample the data to get the last closing price of each day
daily_df = df['Close'].resample('D').last()

# Create a new DataFrame to store the required columns
result_df = pd.DataFrame()
result_df['Date'] = daily_df.index
result_df['Vix Close'] = daily_df.values

# Shift the 'Vix Close' column to get the 'Previous Day Vix Close'
result_df['Previous Day Vix Close'] = result_df['Vix Close'].shift(1)

# Calculate the change in VIX Close compared to the previous day
result_df['VIX Change'] = result_df['Vix Close'] - result_df['Previous Day Vix Close']

# Keep only relevant Columns
result_df = result_df[['Date', 'Vix Close']]

# Drop the first row since it won't have a 'Previous Day Vix Close'
result_df.dropna(inplace=True)

# Reset the index to have a clean DataFrame
result_df.reset_index(drop=True, inplace=True)

# Define the path to save the new CSV file in the "silver" folder
silver_folder = os.path.join(os.path.dirname(__file__), '..', '0-data-silver')
output_file = os.path.join(silver_folder, 'VIX.csv')

# Save the new DataFrame to a CSV file in the "silver" folder
result_df.to_csv(output_file, index=False)

# Display the new DataFrame
print(result_df)