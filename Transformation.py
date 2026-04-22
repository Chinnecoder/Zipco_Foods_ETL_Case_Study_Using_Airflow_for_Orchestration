import pandas as pd

def run_transformation():
# Load the extracted data
data = pd.read_csv(r'zipco_transaction.csv')

# Remove Duplicates
data.drop_duplicates(inplace=True)
print("Duplicates Removed")

# Handling Missing Values (fill missing numeric values with mean or median)
numeric_columns = data.select_dtypes(include=['float64', 'int64']).columns
for col in numeric_columns:
    print(f"Handling missing values in column: {col}")
    data.fillna({col: data[col].mean()}, inplace=True)

# Handling Missing Values(fill missing string or object values with 'Unknown')
string_columns = data.select_dtypes(include=['object']).columns
for col in string_columns:
    print(f"Handling missing values in column: {col}")
    data.fillna({col: 'Unknown'}, inplace=True)

# Cleaning Date column: Convert Date column to datetime format
data['Date'] = pd.to_datetime(data['Date'], errors='coerce')
print("Date Column Converted to datetime format")

# Creating the fact and Dimension Tables

# Create the product table
products = data[['ProductName']].drop_duplicates().reset_index(drop=True)
products.index.name = 'ProductID'
products = products.reset_index()
products.to_csv('products.csv')

# Create the Customer table
customers = data[['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber', 'CustomerEmail']].drop_duplicates().reset_index(drop=True)
customers.index.name = 'CustomerID'
customers = customers.reset_index()
customers.to_csv('customers.csv', index=False)

# Create the Staff table
staff = data[['Staff_Name', 'Staff_Email']].drop_duplicates().reset_index(drop=True)
staff.index.name = 'StaffID'
staff = staff.reset_index()

# Create the Transaction table
transaction = data.merge(products, on=['ProductName'], how='left') \
                  .merge(customers, on=['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber', 'CustomerEmail'], how='left') \
                  .merge(staff, on=['Staff_Name', 'Staff_Email'], how='left')

transaction.index.name = 'TransactionID'
transaction = transaction.reset_index() \
                          [['Date', 'TransactionID', 'ProductID', 'CustomerID', 'StaffID','Quantity', 'UnitPrice', 'StoreLocation',
                            'PaymentType', 'PromotionApplied', 'Weather', 'Temperature','StaffPerformanceRating', 
                            'CustomerFeedback', 'DeliveryTime_min','OrderType', 'DayOfWeek','TotalSales']]

# Save Data as CSV files
data.to_csv('cleaned_data.csv', index=False)
products.to_csv('products.csv', index=False)
customers.to_csv('customers.csv', index=False)
staff.to_csv('staff.csv', index=False)
transaction.to_csv('transaction.csv', index=False)



print("Data Cleaningand Transformation Completed Successfully")
        
     