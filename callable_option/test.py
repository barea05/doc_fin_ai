# import pandas as pd 
# d = {'col1': [1, 2], 'col2': [3, 4]}
# df = pd.DataFrame(data=d)
# print(df)
# print(df.dtypes)

# d = [[1, 2],[3, 4]]
# df = pd.DataFrame(data=d)
# print(df)
# print(df.dtypes)


import pandas as pd

# Sample DataFrames
df1 = pd.DataFrame({
    'A': [1, 2, 3],
    'B': [4, 5, 6]
})

df2 = pd.DataFrame({
    'A': [7, 8, 9],
    'B': [10, 11, 12],
    'C': ['x', 'y', 'z']  # Additional column in df2
})

# Concatenate the DataFrames (default axis=0 for row-wise concatenation)
result = pd.concat([df1, df2], ignore_index=True)

print(result)
