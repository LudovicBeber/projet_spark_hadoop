import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from data import load_game_flow, load_games, load_season
from palette import ACTION_COLORS, BLUE, LEAGUE, OPPONENT, PERIOD_COLORS, TEAM


st.set_page_config(
    page_title="NBA · Le match en données",
    page_icon="🏀",
    layout="wide",
    initial_sidebar_state="expanded",
)

MUTED = "#7B8798"
SEASON_TYPES = {
    "Saison régulière": ["Regular Season"],
    "Playoffs": ["Playoffs"],
    "Les deux": ["Regular Season", "Playoffs"],
}
MARGIN = dict(l=0, r=0, t=10, b=0)

st.markdown(
    """
    <style>
    .block-container {padding-top: 2rem; padding-bottom: 3rem;}
    [data-testid="stSidebar"] {background: #101A2D;}
    [data-testid="stSidebar"] * {color: #F3F6FB;}
    .eyebrow {color: #4B7BEC; font-size: .78rem; font-weight: 700;
              letter-spacing: .12em; text-transform: uppercase;}
    div[data-testid="stMetric"] {background: #F5F7FB; border-radius: 12px;
                                 padding: 16px 18px;}
    </style>
    """,
    unsafe_allow_html=True,
)

try:
    all_games = load_games()
except Exception as exc:  # Mongo injoignable
    st.error(f"MongoDB injoignable : {exc}")
    st.stop()
if all_games.empty:
    st.warning("Aucune donnée Gold dans MongoDB. Lancer d'abord `python3 jobs/gold.py` "
               "depuis le conteneur jupyter.")
    st.stop()

SEASONS = sorted(all_games["season"].unique())

with st.sidebar:
    st.markdown("## NBA / INSIGHTS")
    st.caption("Tableau de bord")
    page = st.radio(
        "Navigation",
        ("Vue générale", "Un match", "Fin de match"),
        label_visibility="collapsed",
    )
    st.divider()
    season = st.selectbox("Saison", SEASONS, index=len(SEASONS) - 1)
    season_type_label = st.radio("Type de match", list(SEASON_TYPES), horizontal=False)
    season_types = SEASON_TYPES[season_type_label]

    in_season = all_games[(all_games["season"] == season)
                          & all_games["season_type"].isin(season_types)]
    team_names = (
        pd.concat([
            in_season[["game_date", "home_abbreviation", "home_team"]]
            .set_axis(["d", "abbr", "name"], axis=1),
            in_season[["game_date", "away_abbreviation", "away_team"]]
            .set_axis(["d", "abbr", "name"], axis=1),
        ])
        .sort_values("d").groupby("abbr")["name"].last().sort_index()
    )
    team_options = ["Toutes les équipes"] + [f"{a} · {n}" for a, n in team_names.items()]
    selected_team_label = st.selectbox("Équipe", team_options)
    selected_team = (
        None if selected_team_label == "Toutes les équipes"
        else selected_team_label.split(" · ", 1)[0]
    )
    st.divider()
    st.caption("Source : HDFS /nba/gold → MongoDB · dataset Kaggle wyattowalsh/basketball")

season_games = in_season
if selected_team:
    season_games = season_games[
        (season_games["home_abbreviation"] == selected_team)
        | (season_games["away_abbreviation"] == selected_team)
    ]
# nombre de "matchs-équipe" : base des moyennes par match
team_games = len(season_games) if selected_team else 2 * len(season_games)


def season_agg(collection):
    df = load_season(collection, season)
    if df.empty:
        return df
    df = df[df["season_type"].isin(season_types)]
    if selected_team:
        df = df[df["team"] == selected_team]
    return df


st.markdown('<div class="eyebrow">Basketball · analyse de match</div>', unsafe_allow_html=True)
st.title(
    {
        "Vue générale": "Le match, minute par minute",
        "Un match": "Explorer un match",
        "Fin de match": "Les moments qui font basculer le match",
    }[page]
)
st.caption(f"{season} · {season_type_label} · {selected_team_label}")


def team_vs_opponent_points(frame, team):
    """Points marqués / encaissés par l'équipe à chaque quart-temps (moyenne)."""
    rows = []
    for q in range(1, 5):
        home = frame["home_abbreviation"] == team
        scored = frame[f"q{q}_home"].where(home, frame[f"q{q}_away"])
        allowed = frame[f"q{q}_away"].where(home, frame[f"q{q}_home"])
        rows += [{"Quart-temps": f"Q{q}", "Série": team, "Points": scored.mean()},
                 {"Quart-temps": f"Q{q}", "Série": "Adversaires", "Points": allowed.mean()}]
    return pd.DataFrame(rows), {team: TEAM, "Adversaires": LEAGUE}


def home_vs_away_points(frame):
    rows = []
    for q in range(1, 5):
        rows += [{"Quart-temps": f"Q{q}", "Série": "Domicile", "Points": frame[f"q{q}_home"].mean()},
                 {"Quart-temps": f"Q{q}", "Série": "Extérieur", "Points": frame[f"q{q}_away"].mean()}]
    return pd.DataFrame(rows), {"Domicile": TEAM, "Extérieur": OPPONENT}


def render_overview():
    if season_games.empty:
        st.info("Aucun match ne correspond à ces filtres.")
        return
    actions = season_agg("team_actions")
    rhythm = season_agg("scoring_rhythm")

    a, b, c, d = st.columns(4)
    a.metric("Matchs", f"{len(season_games):,}".replace(",", " "))
    if selected_team:
        home = season_games["home_abbreviation"] == selected_team
        scored = season_games["home_score"].where(home, season_games["away_score"])
        wins = (season_games["winner_abbreviation"] == selected_team).sum()
        b.metric("Points marqués / match", f"{scored.mean():.1f}")
        c.metric("Bilan", f"{wins} V – {len(season_games) - wins} D")
    else:
        total = season_games["home_score"] + season_games["away_score"]
        home_wins = (season_games["home_score"] > season_games["away_score"]).mean()
        b.metric("Points / match", f"{total.mean():.1f}")
        c.metric("Victoires à domicile", f"{home_wins:.0%}")
    d.metric("Actions (play-by-play)",
             f"{int(actions['n'].sum()):,}".replace(",", " ") if not actions.empty else "–")

    left, right = st.columns([1.45, 1])
    with left:
        st.subheader("Points marqués par quart-temps")
        q = season_games.dropna(subset=[f"q{i}_{s}" for i in range(1, 5) for s in ("home", "away")])
        if q.empty:
            st.info("Scores par quart-temps indisponibles.")
        else:
            data, colors = (team_vs_opponent_points(q, selected_team) if selected_team
                            else home_vs_away_points(q))
            chart = px.bar(data, x="Quart-temps", y="Points", color="Série", barmode="group",
                           color_discrete_map=colors, labels={"Points": "Points / match"})
            chart.update_layout(legend_title_text="", margin=MARGIN)
            st.plotly_chart(chart, use_container_width=True)
    with right:
        st.subheader("Types d’actions")
        if actions.empty:
            st.info("Pas de play-by-play pour cette sélection.")
        else:
            by_action = actions.groupby("action", as_index=False)["n"].sum()
            chart = px.pie(by_action, names="action", values="n", hole=0.62,
                           color="action", color_discrete_map=ACTION_COLORS)
            chart.update_layout(legend_title_text="", margin=MARGIN)
            st.plotly_chart(chart, use_container_width=True)

    st.subheader("Rythme de score")
    if rhythm.empty or not team_games:
        st.info("Pas de play-by-play pour cette sélection.")
        return
    per_minute = rhythm.groupby(["period", "minute"], as_index=False)["points"].sum()
    per_minute["points"] = per_minute["points"] / team_games
    per_minute["Quart-temps"] = per_minute["period"].astype(str)
    chart = px.line(
        per_minute.sort_values(["period", "minute"]),
        x="minute", y="points", color="Quart-temps",
        color_discrete_map=PERIOD_COLORS, markers=True,
        labels={"minute": "Minute du quart-temps",
                "points": "Points / match / équipe" if not selected_team else "Points / match"},
    )
    chart.update_layout(legend_title_text="", margin=MARGIN)
    st.plotly_chart(chart, use_container_width=True)
    st.caption("Points reconstitués depuis le play-by-play (tirs réussis et lancers francs), "
               "moyennés par match.")


def render_game():
    if season_games.empty:
        st.info("Aucun match ne correspond à ces filtres.")
        return

    options = season_games.sort_values("game_date", ascending=False)
    labels = {
        row.game_id: (
            f"{row.game_date:%d/%m/%Y} · {row.home_abbreviation} "
            f"{row.home_score} – {row.away_score} {row.away_abbreviation}"
        )
        for row in options.itertuples()
    }
    game_id = st.selectbox("Choisir un match", options["game_id"].tolist(),
                           format_func=lambda value: labels[value])
    game = season_games[season_games["game_id"] == game_id].iloc[0]
    a, b, c = st.columns([1, 2, 1])
    a.metric(f"{game['home_team']} (dom.)", int(game["home_score"]))
    b.markdown(
        f"<div style='text-align:center;padding-top:22px;color:{MUTED}'>"
        f"{game['game_date']:%d/%m/%Y} · {game['season_type']}</div>",
        unsafe_allow_html=True,
    )
    c.metric(f"{game['away_team']} (ext.)", int(game["away_score"]))

    flow = load_game_flow(game_id)
    if not flow or not flow.get("t"):
        st.info("Pas de play-by-play pour ce match.")
    else:
        t, h, aw = [0.0] + flow["t"], [0] + flow["h"], [0] + flow["a"]
        chart = go.Figure()
        chart.add_trace(go.Scatter(x=t, y=h, mode="lines", name=game["home_abbreviation"],
                                   line=dict(color=TEAM, width=3, shape="hv")))
        chart.add_trace(go.Scatter(x=t, y=aw, mode="lines", name=game["away_abbreviation"],
                                   line=dict(color=OPPONENT, width=3, shape="hv")))
        for m in (12, 24, 36, 48):
            chart.add_vline(x=m, line=dict(color="#D5DAE1", width=1, dash="dot"))
        chart.update_layout(
            title="Évolution du score",
            xaxis_title="Minutes écoulées (prolongations incluses)",
            yaxis_title="Score cumulé",
            hovermode="x unified",
            margin=dict(l=0, r=0, t=50, b=0),
        )
        st.plotly_chart(chart, use_container_width=True)

    st.subheader("Résumé par quart-temps")
    periods = [f"Q{q}" for q in range(1, 5)]
    home_row = [game[f"q{q}_home"] for q in range(1, 5)]
    away_row = [game[f"q{q}_away"] for q in range(1, 5)]
    if game["ot_home"] or game["ot_away"]:
        periods.append("Prol.")
        home_row.append(game["ot_home"])
        away_row.append(game["ot_away"])
    table = pd.DataFrame([home_row + [game["home_score"]], away_row + [game["away_score"]]],
                         columns=periods + ["Total"],
                         index=[game["home_abbreviation"], game["away_abbreviation"]]
                         ).astype("Int64")
    st.dataframe(table, use_container_width=True)


def render_clutch():
    if season_games.empty:
        st.info("Aucun match ne correspond à ces filtres.")
        return
    clutch = season_agg("clutch_team")
    margin = (season_games["home_score"] - season_games["away_score"]).abs()

    a, b, c, d = st.columns(4)
    a.metric("Actions en fin de match", f"{int(clutch['actions'].sum()):,}".replace(",", " ")
             if not clutch.empty else "–")
    b.metric("Fautes en fin de match", f"{int(clutch['fouls'].sum()):,}".replace(",", " ")
             if not clutch.empty else "–")
    shots = clutch["shots"].sum() if not clutch.empty else 0
    c.metric("Réussite au tir en fin de match",
             f"{clutch['shots_made'].sum() / shots:.0%}" if shots else "–")
    d.metric("Matchs décidés par ≤ 5 points", f"{(margin <= 5).sum()} ({(margin <= 5).mean():.0%})")
    st.caption("Fin de match = 2 dernières minutes du 4ᵉ quart-temps et de chaque prolongation. "
               "Une faute n’y est pas forcément volontaire : on compte tous les événements.")

    left, right = st.columns(2)
    with left:
        st.subheader("Points marqués en fin de match, par match")
        all_clutch = load_season("clutch_team", season)
        if all_clutch.empty:
            st.info("Pas de play-by-play pour cette saison.")
        else:
            all_clutch = all_clutch[all_clutch["season_type"].isin(season_types)]
            n_games = pd.concat([in_season["home_abbreviation"],
                                 in_season["away_abbreviation"]]).value_counts()
            by_team = all_clutch.groupby("team", as_index=False)["points"].sum()
            by_team["points / match"] = by_team["points"] / by_team["team"].map(n_games)
            by_team = by_team.sort_values("points / match", ascending=True)
            colors = [TEAM if t == selected_team else (LEAGUE if selected_team else BLUE)
                      for t in by_team["team"]]
            chart = go.Figure(go.Bar(x=by_team["points / match"], y=by_team["team"],
                                     orientation="h", marker_color=colors))
            chart.update_layout(margin=MARGIN, height=max(360, 18 * len(by_team)),
                                xaxis_title="Points / match en fin de match")
            st.plotly_chart(chart, use_container_width=True)
    with right:
        st.subheader("Écart final des matchs")
        chart = px.histogram(margin.rename("écart"), x="écart", nbins=30,
                             color_discrete_sequence=[BLUE],
                             labels={"écart": "Écart de points"})
        chart.update_layout(margin=MARGIN, showlegend=False, yaxis_title="Matchs")
        st.plotly_chart(chart, use_container_width=True)


if page == "Vue générale":
    render_overview()
elif page == "Un match":
    render_game()
else:
    render_clutch()
