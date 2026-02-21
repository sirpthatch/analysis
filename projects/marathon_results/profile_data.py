import pandas as pd
import os

data_file = "/Users/thatcher/dev/analysis/projects/marathon_results/data/race_final/global/data.csv"

print("=" * 80)
print("DATA PROFILE: race_records_final.csv")
print("=" * 80)

# Read the CSV file
print("\nLoading data...")
df = pd.read_csv(data_file)

print(f"\n### OVERALL STATISTICS ###")
print(f"Total rows: {len(df):,}")
print(f"Total columns: {len(df.columns)}")
print(f"Memory usage: {df.memory_usage(deep=True).sum() / (1024**2):.2f} MB")

# Column information
print(f"\n### COLUMN INFORMATION ###")
for col in df.columns:
    dtype = df[col].dtype
    null_count = df[col].isnull().sum()
    null_pct = (null_count / len(df)) * 100
    print(f"  {col:15} - {str(dtype):15} - {null_count:8,} nulls ({null_pct:5.2f}%)")

# Data type summary
print(f"\n### DATA TYPE SUMMARY ###")
print(df.dtypes)

# Statistical summary for numeric columns
print(f"\n### NUMERIC COLUMNS STATISTICS ###")
print(df.describe())

# Sample data
print(f"\n### SAMPLE RECORDS (first 10) ###")
print(df.head(10).to_string())

# City and state analysis
print(f"\n### CITY & STATE BREAKDOWN ###")

# Runners per city-state combination
print(f"\nTotal unique cities: {df['city'].nunique():,}")
print(f"Total unique states: {df['state'].nunique():,}")
print(f"Total unique city-state combinations: {df.groupby(['city', 'state']).size().shape[0]:,}")

# Create city-state summary
city_state_summary = df.groupby(['city', 'state']).size().reset_index(name='runner_count')
city_state_summary = city_state_summary.sort_values('runner_count', ascending=False)

print(f"\n### TOP 50 CITIES BY RUNNER COUNT ###")
print(f"{'City':<30} {'State':<6} {'Runners':>12}")
print("-" * 50)
for idx, row in city_state_summary.head(50).iterrows():
    print(f"{row['city']:<30} {row['state']:<6} {row['runner_count']:>12,}")

# State summary
print(f"\n### RUNNERS BY STATE ###")
state_summary = df.groupby('state').size().reset_index(name='runner_count')
state_summary = state_summary.sort_values('runner_count', ascending=False)

print(f"{'State':<10} {'Runners':>15}")
print("-" * 26)
for idx, row in state_summary.iterrows():
    print(f"{row['state']:<10} {row['runner_count']:>15,}")

# Save the city-state summary to a new file
output_file = "/Users/thatcher/dev/analysis/projects/marathon_results/data/race_final/global/city_state_runner_counts.csv"
city_state_summary.to_csv(output_file, index=False)
print(f"\n✓ City-state breakdown saved to: {output_file}")

# Age and sex analysis
print(f"\n### AGE & SEX BREAKDOWN ###")
print(f"Age range: {df['age'].min():.0f} to {df['age'].max():.0f}")
print(f"Mean age: {df['age'].mean():.1f}")
print(f"Median age: {df['age'].median():.1f}")
print(f"\nSex distribution:")
print(df['sex'].value_counts())

# Time analysis
print(f"\n### TIME STATISTICS (in minutes) ###")
print(f"Min time: {df['time'].min():.1f} minutes ({df['time'].min()/60:.1f} hours)")
print(f"Max time: {df['time'].max():.1f} minutes ({df['time'].max()/60:.1f} hours)")
print(f"Mean time: {df['time'].mean():.1f} minutes ({df['time'].mean()/60:.1f} hours)")
print(f"Median time: {df['time'].median():.1f} minutes ({df['time'].median()/60:.1f} hours)")

print(f"\n### RACE DISTRIBUTION ###")
race_counts = df['race'].value_counts()
print(f"Total unique races: {len(race_counts):,}")
print(f"\nTop 20 races by runner count:")
print(f"{'Race':<50} {'Runners':>12}")
print("-" * 64)
for race, count in race_counts.head(20).items():
    print(f"{race:<50} {count:>12,}")

print("\n" + "=" * 80)
print("Profile complete!")
print("=" * 80)
