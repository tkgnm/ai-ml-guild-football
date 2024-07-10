import pandas as pd

# Load your data
goalscorers = pd.read_csv('data/goalscorers.csv')
results = pd.read_csv('data/results.csv')
shootouts = pd.read_csv('data/shootouts.csv')

print(f"Lengths of each csv: {len(goalscorers)}, {len(results)}, {len(shootouts)}")

# Check for missing values
print("Missing values in results key columns:\n", results[['date', 'home_team', 'away_team', 'home_score', 'away_score']].isnull().sum())
print("Missing values in goalscorers key columns:\n", goalscorers[['date', 'home_team', 'away_team']].isnull().sum())
print("Missing values in shootouts key columns:\n", shootouts[['date', 'home_team', 'away_team']].isnull().sum())

# There are some missing values. This is because the results csv has rows for fixtures that have not been played yet.
# Let's drop those from the data.
results.dropna(subset=['date', 'home_team', 'away_team', 'home_score', 'away_score'], inplace=True)
goalscorers.dropna(subset=['date', 'home_team', 'away_team'], inplace=True)
shootouts.dropna(subset=['date', 'home_team', 'away_team'], inplace=True)

# Merge datasets on 'date', 'home_team', and 'away_team' using an outer join
merged_data = results.merge(goalscorers, on=['date', 'home_team', 'away_team'], how='outer')
merged_data = merged_data.merge(shootouts, on=['date', 'home_team', 'away_team'], how='outer')

# Check for missing data
missing_from_results = results[~results.set_index(['date', 'home_team', 'away_team']).index.isin(merged_data.set_index(['date', 'home_team', 'away_team']).index)]
missing_from_goalscorers = goalscorers[~goalscorers.set_index(['date', 'home_team', 'away_team']).index.isin(merged_data.set_index(['date', 'home_team', 'away_team']).index)]
missing_from_shootouts = shootouts[~shootouts.set_index(['date', 'home_team', 'away_team']).index.isin(merged_data.set_index(['date', 'home_team', 'away_team']).index)]

print("Missing from results:", missing_from_results.shape[0])
print("Missing from goalscorers:", missing_from_goalscorers.shape[0])
print("Missing from shootouts:", missing_from_shootouts.shape[0])

if not missing_from_results.empty:
    print("Details of missing records from results:\n", missing_from_results)
if not missing_from_goalscorers.empty:
    print("Details of missing records from goalscorers:\n", missing_from_goalscorers)
if not missing_from_shootouts.empty:
    print("Details of missing records from shootouts:\n", missing_from_shootouts)

# Save to CSV
print("Saving merged data frame to disk...")
merged_data.to_csv("merged_data.csv", index=False)