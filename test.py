import pandas as pd

# Your dataframe
data = {
    'System Id': [1, 1, 1, 1, 1],
    'System Name': ['CB', 'CB', 'CB', 'CB', 'CB'],
    'Stream Key': [1, 2, 3, 4, 5],
    'Stream Name': ['CB_STG', 'CB_BKEY', 'CB_BMAP', 'CB_SRCI', 'CB_CORE'],
    'Loading Frequency': [1, 1, 1, 1, 1]
}

df = pd.DataFrame(data)

# Your input string
input_string = "This is a BKEY example"  # or whatever string you have

# Extract substrings after underscore
substrings = df['Stream Name'].str.split('_').str[1]

# Check which substrings are contained in your input string
mask = substrings.str.lower().apply(lambda x: x.lower() in input_string.lower())

# Filter the dataframe
filtered_df = df[mask]

print(filtered_df)