import pandas as pd

# Read the CSV file into a DataFrame
df = pd.read_csv('merged_researches.csv')

# Split rows where subject_areas contain multiple values separated by "|"
split_df = df.assign(subject_areas=df['subject_areas'].str.split('|')).explode('subject_areas')

# Reset index to have consecutive row numbers
split_df.reset_index(drop=True, inplace=True)

# Save the modified DataFrame to a new CSV file
split_df.to_csv('split_subject_areas.csv', index=False)