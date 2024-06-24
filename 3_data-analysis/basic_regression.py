import pandas as pd
import statsmodels.api as sm
import os

# Read the CSV file
# Paths
gold_path = os.path.join(os.path.dirname(__file__), '..', '0-data-gold')   
csv_file = os.path.join(gold_path, 'data.csv')

# Load the CSV file into a pandas DataFrame
df = pd.read_csv(csv_file)

# Ensure the 'End of Week Date' column is in datetime format if necessary
df['Date'] = pd.to_datetime(df['Date'])

# Define the dependent variable
y = df['% FTSE100 Change']

# Define the independent variables, including 'Change in SWAV' and the other control variables
X = df[['ADS Change', 'EPU Change', 'Previous Day % FTSE100 Change', '% MSCI Change', 'Vix Close', 'Change in SWAV']]

# Add a constant to the model (intercept)
X = sm.add_constant(X)

# Build and fit the regression model
model = sm.OLS(y, X).fit()

# Print the summary of the regression model
print(model.summary())