"""Thin shim that points legacy step006 entrypoint to legacy_steps."""

from golgg.legacy_steps.step006_ranking_players import main


if __name__ == "__main__":
    main()



# =============================================================================
##### Tentativa player grades por role, porÃ©m perde o valor entre roles e isola cada um, 
##### mesmo alguns valores serem "injustos" entre roles.

# for role in df['Role'].unique():
#     
#     dfrole = df[df['Role']==role]
#     player_avg = dfrole.groupby('Player').mean(numeric_only=True)
#     player_grade = player_avg.apply(calculate_grade, axis=0)
#     
#     player_match_count = dfrole['Player'].value_counts().rename('Match_count')
#     player_grade = player_grade.join(player_match_count)
#     
#     player_grade['Rank'] = player_grade.sum(axis=1).rank(ascending=False)
#     player_grade['Role'] = role
#     columns = player_grade.columns.tolist()
#     columns.remove('Role')
#     columns.insert(0, 'Role')
#     player_grade = player_grade[columns]
#     player_grade = player_grade.sort_values(by='Rank', ascending=True)
# =============================================================================



# =============================================================================
# weights = {
#     "Level": 1.0, "Kills": 1.0, "Deaths": 1.0, "Assists": 1.0, "CS": 1.0, "CS in Team's Jungle": 1.0, "CS in Enemy Jungle": 1.0, "CSM": 1.0, "Golds": 1.0, "GPM": 1.0, "Vision Score": 1.0, "Wards placed": 1.0, "Wards destroyed": 1.0, "Control Wards Purchased": 1.0, "Detector Wards Placed": 1.0, "VSPM": 1.0, "WPM": 1.0, "VWPM": 1.0, "WCPM": 1.0, "Total damage to Champion": 1.0, "Physical Damage": 1.0, "Magic Damage": 1.0, "True Damage": 1.0, "DPM": 1.0, "K+A Per Minute": 1.0, "Solo kills": 1.0, "Double kills": 1.0, "Triple kills": 1.0, "Quadra kills": 1.0, "Penta kills": 1.0, "GD@15": 1.0, "CSD@15": 1.0, "XPD@15": 1.0, "LVLD@15": 1.0, "Objectives Stolen": 1.0, "Damage dealt to turrets": 1.0, "Damage dealt to buildings": 1.0, "Total heal": 1.0, "Total Heals On Teammates": 1.0, "Damage self mitigated": 1.0, "Total Damage Shielded On Teammates": 1.0, "Time ccing others": 1.0, "Total Time CC Dealt": 1.0, "Total damage taken": 1.0, "Total Time Spent Dead": 1.0, "Consumables purchased": 1.0, "Items Purchased": 1.0, "Shutdown bounty collected": 1.0, "Shutdown bounty lost": 1.0
# }
# 
# # Calculate the overall grade based on the weighted average of the metrics
# player_avg['Overall_Grade'] = (player_avg[list(weights.keys())] * list(weights.values())).sum(axis=1)
# 
# # Calculate the percentile rank for each column
# for col in df.columns[1:-2]:  # Exclude 'Player', 'Level', 'Overall_Grade', and 'Rank' columns
#     df[col + '_Percentile'] = df.groupby('Player')[col].rank(pct=True)
# 
# # Calculate the overall grade as the average of the percentile ranks for all metrics
# metrics_cols = [col for col in df.columns if col.endswith('_Percentile')]
# df['Overall_Grade'] = df[metrics_cols].mean(axis=1) * 100
# 
# # Sort the players based on the percentile-based 'Overall_Grade' column in descending order
# player_avg = round(df.groupby('Player')['Overall_Grade'].mean().reset_index(), 2)
# player_avg = player_avg.sort_values(by='Overall_Grade', ascending=False)
# player_avg['Rank'] = range(1, len(player_avg) + 1)
# 
# dfx = pd.DataFrame({'count' : df.groupby( [ "Player"] ).size()}).reset_index()
# player_avg = player_avg.merge(dfx, on='Player')
# 
# # Calculate the weighted average overall grade, considering both average performance and number of matches
# weight_average_grade = (player_avg['Overall_Grade'] * player_avg['count']).sum() / player_avg['count'].sum()
# 
# # Normalize the overall grade to a 0.00 to 100.00 scale for each player
# min_grade = player_avg['Overall_Grade'].min()
# max_grade = player_avg['Overall_Grade'].max()
# player_avg['Normalized_Grade'] = 100 * (player_avg['Overall_Grade'] - min_grade) / (max_grade - min_grade)
# 
# # Penalty term for players with a lower number of matches (adjust the weight as needed)
# penalty_weight = 0.5
# player_avg['Penalty'] = 1 - (player_avg['count'] / player_avg['count'].max()) * penalty_weight
# 
# # Calculate the final weighted average normalized grade, including the penalty term
# player_avg['Final_Normalized_Grade'] = player_avg['Normalized_Grade'] * player_avg['Penalty']
# 
# # Sort the players based on the final weighted average normalized grades to get the rankings
# player_avg = player_avg.sort_values(by='Final_Normalized_Grade', ascending=False)
# player_avg['Rank'] = range(1, len(player_avg) + 1)
# 
# # Save the relevant columns to a new CSV file
# player_avg.to_csv('golgg/source/final_player_grades.csv', columns=['Player', 'Final_Normalized_Grade', 'Rank', 'count'], index=False)
# 
# # player_avg.to_csv('golgg/source/player_grades_normalized.csv', index=False)
# =============================================================================







