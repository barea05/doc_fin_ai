# import pandas as pd 


# import pandas as pd

# # Example DataFrame with duplicate column names
# data = [
#     'amount': [100, 150, 200],
#     'yield': [0.05, 0.06, 0.05],
#     'price': [99, 98, 100],
#     'cusip': ['714376PP9', '714376PP9', '714376PP9'],
#     'maturity': ['2025', '2026', '2027'],
#     'amount': [250, 300, 350],  # duplicate 'amount'
#     'interest_rate': [0.03, 0.04, 0.05],
#     'yield': [0.06, 0.07, 0.06],  # duplicate 'yield'
#     'price': [97, 96, 99],  # duplicate 'price'
#     'cusip': ['714376PP9', '714376PP9', '714376PP9']  # duplicate 'cusip'
# }

# # Create the DataFrame
# df = pd.DataFrame(data)

# # Display the original DataFrame
# print("Original DataFrame:")
# print(df)

# # Step 1: Identify duplicate column names
# duplicates = [col for col in df.columns if df.columns.tolist().count(col) > 1]

# # Step 2: Iterate over the duplicates and concatenate their values
# for col in set(duplicates):
#     # Get all columns with the same name
#     cols_to_concat = [column for column in df.columns if column == col]
    
#     if len(cols_to_concat) > 1:
#         # Concatenate the values from the duplicate columns
#         combined_values = pd.concat([df[col] for col in cols_to_concat], axis=0).reset_index(drop=True)
        
#         # Set the first occurrence to NaN
#         df[col] = pd.concat([pd.Series([float('nan')] * len(df)), combined_values[len(df):]]).reset_index(drop=True)

#         # Drop the extra duplicate columns
#         df = df.drop(columns=cols_to_concat[1:])

# # Display the modified DataFrame with concatenated values for duplicates
# print("\nModified DataFrame:")
# print(df)


import pandas as pd

# Sample DataFrame
data = {
    'amount': ['2022 $60,000', '2023 60,000', '2024 60,000', '2025 60,000', '2026 60,000', '$275,000  2.000% Term Bonds Due June 1, 2035'],
    'yield': ['3.00%', '3.00%', '3.00%', '3.00%', '3.00%', 'NaN'],
    'price': ['102.889%', '105.415%', '107.722%', '109.594%', '111.398%', 'NaN'],
    'cusip': ['AA0', 'AB8', 'AC6', 'AD4', 'AE2', 'NaN'],
    'interest_rate': ['3.00%', '3.00%', '3.00%', '3.00%', 'NaN', 'NaN'],
    'yield': ['0.250%', '0.350%', '0.450%', '0.600%', '0.700%', 'NaN'],
    'price': ['112.343%', '113.586%', '114.189%', '115.396%', 'NaN', 'NaN'],
    'cusip': ['AF9', 'AG7', 'AH5', 'AJ1', 'NaN', 'NaN']
}

# Creating the DataFrame
df = pd.DataFrame(data)
print(df)
# Step 1: Find and merge duplicate columns by column name
# We'll use a dictionary to aggregate values of duplicate columns into a single column

# Iterate over the columns and combine duplicates
for col in df.columns:
    if df.columns.tolist().count(col) > 1:  # If the column is duplicated
        # Merge the duplicates by filling NaNs from other columns with non-null values
        merged_column = df.filter(like=col).bfill(axis=1).iloc[:, 0]  # Merge along rows, backward fill
        df[col] = merged_column  # Assign the merged column back to the DataFrame
        # Drop the original duplicates
        df = df.loc[:, ~df.columns.duplicated()]

# Show the cleaned DataFrame
print(df)
