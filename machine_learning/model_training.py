import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score

# Load and merge data (create win/loss label from wins/losses ratio or netrunrate >0 as win proxy)
file_path_raw = r'..\databricks\gold\data\ipl_team_season.csv'
df_team = pd.read_csv(file_path_raw)

df_team['win'] = (df_team['wins'] > df_team['losses']).astype(int)
df = df_team[['teamsk', 'season', 'netrunrate', 'avgrunsscored', 'win']].dropna()

le = LabelEncoder()
X = pd.get_dummies(df[['teamsk', 'season']], drop_first=True)
X['netrunrate'] = df['netrunrate']
X['avgrunsscored'] = df['avgrunsscored']
y = df['win']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
model = LogisticRegression().fit(X_train, y_train)
preds = model.predict(X_test)
print("Accuracy:", accuracy_score(y_test, preds))  # Typical 70-80% [web:39][web:45]

import joblib
joblib.dump(model, 'ipl_win_model.joblib')
joblib.dump(le, 'label_encoder.joblib')
