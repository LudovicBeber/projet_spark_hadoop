"""Accès aux collections Gold dans MongoDB (alimentées par jobs/gold.py)."""
import os

import pandas as pd
import streamlit as st
from pymongo import MongoClient

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.environ.get("MONGO_DB", "nba")


@st.cache_resource
def _db():
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)[MONGO_DB]


@st.cache_data(ttl=600, show_spinner="Chargement des matchs…")
def load_games():
    """Tous les matchs couverts par le play-by-play (≈ 35 000 lignes)."""
    docs = _db()["games"].find({"has_pbp": True}, {"_id": 0})
    df = pd.DataFrame(list(docs))
    if not df.empty:
        df["game_date"] = pd.to_datetime(df["game_date"])
    return df


@st.cache_data(ttl=600)
def load_season(collection, season):
    """Agrégats Gold d'une saison (team_actions, scoring_rhythm, clutch_team)."""
    return pd.DataFrame(list(_db()[collection].find({"season": season}, {"_id": 0})))


@st.cache_data(ttl=600)
def load_game_flow(game_id):
    return _db()["game_flow"].find_one({"game_id": game_id}, {"_id": 0})
