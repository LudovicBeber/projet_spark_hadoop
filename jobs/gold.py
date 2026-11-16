"""Couche Gold : HDFS /nba/silver -> HDFS /nba/gold (parquet) + MongoDB (service).

Tables produites (une collection MongoDB par table, base `nba`) :
  games          1 ligne par match (saison régulière + playoffs), scores par QT
  team_actions   nb d'actions par saison / type / équipe / type d'action
  scoring_rhythm points marqués par saison / type / équipe / QT / minute
  clutch_team    fin de match (2 dernières min du Q4 et des prolongations)
  game_flow      1 document par match : courbe du score (minutes, dom., ext.)

Le dashboard Streamlit lit uniquement MongoDB.
"""
import os

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F

SILVER = "hdfs://namenode:9000/nba/silver"
GOLD = "hdfs://namenode:9000/nba/gold"
REPORT = "/work/resultats/controles_gold.md"
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.environ.get("MONGO_DB", "nba")

SEASON_TYPES = ["Regular Season", "Playoffs"]
CLUTCH_SECONDS = 120

# codes eventmsgtype NBA
MADE, MISSED, FREE_THROW, REBOUND, TURNOVER, FOUL, TIMEOUT = 1, 2, 3, 4, 5, 6, 9


def season_label(annee):
    return F.concat(annee.cast("string"), F.lit("-"),
                    F.lpad(((annee + 1) % 100).cast("string"), 2, "0"))


def build_games(spark):
    g = spark.read.parquet(f"{SILVER}/games").filter(
        F.col("season_type").isin(SEASON_TYPES) & F.col("game_id").isNotNull()
        & F.col("pts_home").isNotNull() & F.col("pts_away").isNotNull())
    pbp_ids = (spark.read.parquet(f"{SILVER}/play_by_play").select("game_id").distinct()
               .withColumn("has_pbp", F.lit(True)))

    def ot(side):
        return sum(F.coalesce(F.col(f"pts_ot{i}_{side}"), F.lit(0)) for i in range(1, 11))

    quarters = [F.col(f"pts_qtr{q}_{s}").cast("int").alias(f"q{q}_{s}")
                for s in ("home", "away") for q in range(1, 5)]
    return (g.join(F.broadcast(pbp_ids), "game_id", "left")
             .select(
                 "game_id", "annee", season_label(F.col("annee")).alias("season"),
                 "season_type", F.col("game_date").cast("timestamp").alias("game_date"),
                 F.col("team_abbreviation_home").alias("home_abbreviation"),
                 F.col("team_name_home").alias("home_team"),
                 F.col("team_abbreviation_away").alias("away_abbreviation"),
                 F.col("team_name_away").alias("away_team"),
                 F.col("pts_home").cast("int").alias("home_score"),
                 F.col("pts_away").cast("int").alias("away_score"),
                 F.when(F.col("pts_home") > F.col("pts_away"), F.col("team_abbreviation_home"))
                  .otherwise(F.col("team_abbreviation_away")).alias("winner_abbreviation"),
                 *quarters,
                 ot("home").cast("int").alias("ot_home"),
                 ot("away").cast("int").alias("ot_away"),
                 F.col("attendance").cast("int").alias("attendance"),
                 F.coalesce("has_pbp", F.lit(False)).alias("has_pbp")))


def build_events(spark, games):
    """Play-by-play enrichi : équipe, type d'action, points, minute, fin de match."""
    g = games.filter("has_pbp").select("game_id", "season", "season_type",
                                       "home_abbreviation", "away_abbreviation")
    p = spark.read.parquet(f"{SILVER}/play_by_play").join(F.broadcast(g), "game_id")

    hd, vd = F.col("homedescription"), F.col("visitordescription")
    side = (F.when(F.col("person1type").isin(2, 4), "home")
             .when(F.col("person1type").isin(3, 5), "away")
             .when(hd.isNotNull() & vd.isNull(), "home")
             .when(vd.isNotNull() & hd.isNull(), "away"))
    p = p.withColumn("side", side)
    desc = F.upper(F.coalesce(F.when(F.col("side") == "home", hd)
                               .when(F.col("side") == "away", vd), F.lit("")))
    t = F.col("eventmsgtype")
    is_shot = t.isin(MADE, MISSED)
    action = (F.when(is_shot & desc.contains("3PT"), "Tir à 3 points")
               .when(is_shot & desc.contains("DUNK"), "Dunk")
               .when(is_shot & desc.contains("LAYUP"), "Lay-up")
               .when(is_shot, "Autre tir à 2 pts")
               .when(t == FREE_THROW, "Lancer franc")
               .when(t == REBOUND, "Rebond")
               .when(t == TURNOVER, "Perte de balle")
               .when(t == FOUL, "Faute")
               .when(t == TIMEOUT, "Temps mort"))
    points = (F.when((t == MADE) & desc.contains("3PT"), 3)
               .when(t == MADE, 2)
               .when((t == FREE_THROW) & ~desc.contains("MISS"), 1)
               .otherwise(0))
    period_len = F.when(F.col("period") <= 4, 720).otherwise(300)
    minute = F.least(F.floor((period_len - F.col("rest")) / 60) + 1, period_len / 60)
    return (p.select(
                "game_id", "annee", "season", "season_type", "eventnum", "period",
                "rest", "sec", "score_ok", "score_home", "score_away",
                F.col("eventmsgtype").alias("t"), "side",
                F.when(F.col("side") == "home", F.col("home_abbreviation"))
                 .when(F.col("side") == "away", F.col("away_abbreviation")).alias("team"),
                action.alias("action"), points.alias("points"),
                minute.cast("int").alias("minute"),
                ((F.col("period") >= 4) & (F.col("rest") <= CLUTCH_SECONDS)).alias("clutch"))
             .persist(StorageLevel.DISK_ONLY))


def aggregates(ev):
    keys = ["annee", "season", "season_type", "team"]
    t = F.col("t")
    team_actions = (ev.filter(F.col("action").isNotNull() & F.col("team").isNotNull())
                      .groupBy(*keys, "action").agg(F.count("*").alias("n")))
    scoring_rhythm = (ev.filter((F.col("points") > 0) & (F.col("period") <= 4)
                                & F.col("minute").isNotNull() & F.col("team").isNotNull())
                        .groupBy(*keys, "period", "minute")
                        .agg(F.sum("points").cast("int").alias("points")))
    clutch_team = (ev.filter(F.col("clutch") & F.col("team").isNotNull())
                     .groupBy(*keys).agg(
                         F.count("*").alias("actions"),
                         F.sum(F.when(t == FOUL, 1).otherwise(0)).alias("fouls"),
                         F.sum("points").cast("int").alias("points"),
                         F.sum(F.when(t.isin(MADE, MISSED), 1).otherwise(0)).alias("shots"),
                         F.sum(F.when(t == MADE, 1).otherwise(0)).alias("shots_made"),
                         F.countDistinct("game_id").alias("games")))
    flow = (ev.filter(F.col("score_ok") & F.col("sec").isNotNull())
              .groupBy("game_id")
              .agg(F.array_sort(F.collect_list(
                  F.struct("sec", "eventnum", "score_home", "score_away"))).alias("pts")))
    game_flow = flow.select(
        "game_id",
        F.transform("pts", lambda x: F.round(x["sec"] / 60, 2)).alias("t"),
        F.transform("pts", lambda x: x["score_home"]).alias("h"),
        F.transform("pts", lambda x: x["score_away"]).alias("a"))
    return {"team_actions": team_actions, "scoring_rhythm": scoring_rhythm,
            "clutch_team": clutch_team, "game_flow": game_flow}


def to_mongo(df, coll, indexes=()):
    """Remplace la collection `coll` ; insertion parallèle depuis les exécuteurs."""
    from pymongo import MongoClient
    uri, dbname = MONGO_URI, MONGO_DB
    with MongoClient(uri) as c:
        c[dbname][coll].drop()
        for idx in indexes:
            c[dbname][coll].create_index(idx)

    def write_partition(rows):
        from pymongo import MongoClient
        with MongoClient(uri) as cl:
            col, batch = cl[dbname][coll], []
            for r in rows:
                batch.append(r.asDict(recursive=True))
                if len(batch) >= 2000:
                    col.insert_many(batch, ordered=False)
                    batch = []
            if batch:
                col.insert_many(batch, ordered=False)

    df.foreachPartition(write_partition)
    with MongoClient(uri) as c:
        return c[dbname][coll].estimated_document_count()


def main():
    spark = (SparkSession.builder
             .appName("nba-gold")
             .master("spark://spark-master:7077")
             .config("spark.executor.cores", "2")
             .config("spark.executor.memory", "2g")
             .config("spark.sql.shuffle.partitions", "48")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    games = build_games(spark)
    games.write.mode("overwrite").parquet(f"{GOLD}/games")
    games = spark.read.parquet(f"{GOLD}/games").cache()

    ev = build_events(spark, games)
    tables = {"games": games, **aggregates(ev)}

    # contrôle : points reconstitués depuis le play-by-play vs score officiel
    pts = (ev.groupBy("game_id").agg(
              F.sum(F.when(F.col("side") == "home", F.col("points")).otherwise(0)).alias("ph"),
              F.sum(F.when(F.col("side") == "away", F.col("points")).otherwise(0)).alias("pa"))
             .join(games.select("game_id", "home_score", "away_score"), "game_id"))
    chk = pts.agg(F.count("*").alias("n"),
                  F.sum(F.when((F.col("ph") == F.col("home_score"))
                               & (F.col("pa") == F.col("away_score")), 1)
                        .otherwise(0)).alias("ok")).first()

    indexes = {"games": [[("season", 1), ("season_type", 1)], "game_id"],
               "team_actions": [[("season", 1), ("team", 1)]],
               "scoring_rhythm": [[("season", 1), ("team", 1)]],
               "clutch_team": [[("season", 1), ("team", 1)]],
               "game_flow": ["game_id"]}
    counts = {}
    for name, df in tables.items():
        if name != "games":
            df.write.mode("overwrite").parquet(f"{GOLD}/{name}")
            df = spark.read.parquet(f"{GOLD}/{name}")
        counts[name] = to_mongo(df, name, indexes[name])
        print(f"{name}: {counts[name]} documents -> mongo {MONGO_DB}.{name}")
    ev.unpersist()

    seasons = (games.filter("has_pbp").groupBy("season", "season_type").count()
                    .orderBy("season", "season_type").collect())
    L = ["# Contrôles Gold\n", "## Tables (HDFS /nba/gold + MongoDB `nba`)\n",
         "| table | documents |", "|---|---|"]
    L += [f"| {k} | {v:,} |".replace(",", " ") for k, v in counts.items()]
    L += ["\n## Cohérence play-by-play / score officiel\n",
          f"Matchs dont la somme des points du play-by-play (tirs réussis + lancers francs) "
          f"égale exactement le score final des deux équipes : **{chk.ok:,} / {chk.n:,}** "
          f"({chk.ok / max(chk.n, 1):.1%}).".replace(",", " "),
          "\n## Matchs avec play-by-play par saison\n",
          "| saison | type | matchs |", "|---|---|---|"]
    L += [f"| {r.season} | {r.season_type} | {r['count']} |" for r in seasons]
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w", encoding="utf-8") as f:
        f.write("\n".join(L))
    print(f"coherence points pbp/score : {chk.ok}/{chk.n}")
    print(f"rapport : {REPORT}")
    spark.stop()


if __name__ == "__main__":
    main()
