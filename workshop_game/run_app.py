import streamlit as st
import pandas as pd
import pickle
import random

st.title("AI vs Hydrologists")

# load LSTM results
path = r"C:\Users\hdagne1\Box\NRT_Project_2026_Spring\Habtamu\HydroAuditToolFrameowrk\runs\run_1303_1312_seed77391\lstm_seed77391.p"

with open(path,"rb") as f:
    results = pickle.load(f)

df = pd.concat(results,names=["basin","date"]).reset_index()
df["date"] = pd.to_datetime(df["date"])

# session state for scores
if "ai_score" not in st.session_state:
    st.session_state.ai_score = 0
if "hydro_score" not in st.session_state:
    st.session_state.hydro_score = 0

# random sample
row = df.sample(1).iloc[0]

basin = row["basin"]
date = row["date"]
qobs = float(row["qobs"])
qsim = float(row["qsim"])

st.subheader(f"Basin {basin}")
st.write(f"Date: {date.date()}")

st.metric("LSTM Prediction", f"{qsim:.2f}")

st.write("Observed flow is hidden.")

col1,col2 = st.columns(2)

vote_ai = col1.button("Trust AI")
vote_hydro = col2.button("AI is Wrong")

if vote_ai or vote_hydro:

    st.write(f"Observed Flow: {qobs:.2f}")

    ai_correct = abs(qsim - qobs) < (0.5*qobs)

    if vote_ai:
        if ai_correct:
            st.success("AI team wins this round!")
            st.session_state.ai_score += 1
        else:
            st.error("Hydrologists were right!")
            st.session_state.hydro_score += 1

    if vote_hydro:
        if not ai_correct:
            st.success("Hydrologists win this round!")
            st.session_state.hydro_score += 1
        else:
            st.error("AI prediction was correct!")
            st.session_state.ai_score += 1

st.write("---")

st.write("Scoreboard")

st.write(f"AI Team: {st.session_state.ai_score}")
st.write(f"Hydrologists: {st.session_state.hydro_score}")