import streamlit as st
import joblib
import pandas as pd
import numpy as np

@st.cache_resource
def load_model():
    model = joblib.load('ipl_win_model.joblib')
    return model

st.title("IPL Win/Loss Predictor")
model = load_model()

teamsk = st.selectbox("Team SK", options=[1,6,9,11,12])  # From sample data
season = st.number_input("Season", min_value=2008, max_value=2026, value=2017)
netrunrate = st.number_input("Net Run Rate", value=0.0)
avgrunsscored = st.number_input("Avg Runs Scored", value=150.0)

if st.button("Predict"):
    input_df = pd.DataFrame({
        'teamsk': [teamsk], 'season': [season],
        'netrunrate': [netrunrate], 'avgrunsscored': [avgrunsscored]
    })
    # Mimic training preprocessing (adjust columns as per your X)
    X_input = pd.get_dummies(input_df[['teamsk', 'season']], drop_first=True)
    X_input['netrunrate'] = input_df['netrunrate']
    X_input['avgrunsscored'] = input_df['avgrunsscored']
    # Reindex to match training columns (fill missing with 0)
    X_input = X_input.reindex(columns=model.feature_names_in_, fill_value=0)
    
    pred = model.predict(X_input)[0]
    prob = model.predict_proba(X_input)[0]
    st.success(f"Prediction: {'Win' if pred == 1 else 'Loss'} (Prob: {prob[1]:.2%} Win)") [web:99][web:102]
