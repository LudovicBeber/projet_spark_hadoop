import argparse
import os
import sys

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F, Window

sys.path.insert(0, os.path.dirname(__file__))
from schemas import SCHEMAS

CSV = "hdfs://namenode:9000/nba/raw"
OUT = "hdfs://namenode:9000/nba/silver"
Q = f"{OUT}/_quarantine"
STAGING_PBP = f"{OUT}/_staging/play_by_play_typed"
REPORT = "/work/resultats/controles_silver.md"

PERIOD_MAX = 14
PBP_KEY = ["game_id", "eventnum"]


def normalize_season_type(df):
    st = F.lower(F.trim(F.regexp_replace("season_type", "[\\-_]", " ")))
    return df.withColumn("season_type",
        F.when(st.like("%all%star%"), F.lit("All-Star"))
         .when(st.like("%regular%"), F.lit("Regular Season"))
         .when(st.like("%playoff%"), F.lit("Playoffs"))
         .when(st.like("%pre%season%"), F.lit("Pre Season"))
         .when(st.like("%cup%"), F.lit("NBA Cup"))
         .otherwise(F.col("season_type")))


def split_key_conflicts(df, keys):
    """Après dédoublonnage exact : une clé encore présente plusieurs fois
    = lignes contradictoires -> quarantaine."""
    df = df.withColumn("_n", F.count(F.lit(1)).over(Window.partitionBy(*keys)))
    return df.filter("_n = 1").drop("_n"), df.filter("_n > 1").drop("_n")


def parse_clock(df):
    m = F.split("pctimestring", ":")
    ok = F.col("pctimestring").rlike("^[0-9]+:[0-9]{2}$")
    return (df.withColumn("clock_ok", F.coalesce(ok, F.lit(False)))
              .withColumn("rest", F.when(ok, m.getItem(0).cast("int") * 60
                                          + m.getItem(1).cast("int"))))


def parse_score(df):
    # format NBA : "VISITEUR - DOMICILE"
    ok = F.col("score").rlike("^ *[0-9]+ *- *[0-9]+ *$")
    s = F.split("score", "-")
    return (df.withColumn("score_ok", F.coalesce(ok, F.lit(False)))
              .withColumn("score_away", F.when(ok, F.trim(s.getItem(0)).cast("int")))
              .withColumn("score_home", F.when(ok, F.trim(s.getItem(1)).cast("int"))))


def normalize_pbp_types(df):
    """Corrige les artefacts de typage du CSV play_by_play :
    person{i}type "4.0" -> 4, player{i}_team_id "1610612747.0" -> "1610612747",
    scoremargin "TIE" -> 0."""
    for i in (1, 2, 3):
        df = (df.withColumn(f"person{i}type", F.col(f"person{i}type").cast("int"))
                .withColumn(f"player{i}_team_id",
                            F.regexp_replace(f"player{i}_team_id", "\\.0$", "")))
    sm = F.trim("scoremargin")
    return df.withColumn("scoremargin",
                         F.when(F.upper(sm) == "TIE", F.lit(0)).otherwise(sm.cast("int")))


def write_parquet(df, path, part=None):
    w = df.write.mode("overwrite")
    (w.partitionBy(part) if part else w).parquet(path)


def hdfs_exists(spark, path):
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    return p.getFileSystem(spark._jsc.hadoopConfiguration()).exists(p)


def md_table(headers, rows):
    out = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def read_csv(spark, name):
    return (spark.read.schema(SCHEMAS[name]).option("header", "true")
            .csv(f"{CSV}/{name}.csv"))


def align_home_away(ls, game, cols, R):
    """Dans line_score, ~47 % des matchs ont domicile/extérieur inversés par
    rapport à game (équipes ET points). On réaligne sur game.team_id_home."""
    ls = ls.join(game.select("game_id", F.col("team_id_home").alias("_g_home")),
                 "game_id", "left")
    swap = F.coalesce(F.col("team_id_home") != F.col("_g_home"), F.lit(False))
    R["line_score_inversions_dom_ext"] = ls.filter(swap).count()
    out = [F.col("game_id")]
    for h in (c for c in cols if c.endswith("_home")):
        a = h[:-len("_home")] + "_away"
        out += [F.when(swap, F.col(a)).otherwise(F.col(h)).alias(h),
                F.when(swap, F.col(h)).otherwise(F.col(a)).alias(a)]
    return ls.select(*out)


def build_games(spark, R):
    game = normalize_season_type(read_csv(spark, "game")).cache()
    R["game_lignes"] = game.count()
    game = game.dropDuplicates().cache()
    R["game_doublons_exact"] = R["game_lignes"] - game.count()
    game, gconf = split_key_conflicts(game, ["game_id"])
    gconf = gconf.cache()
    R["game_conflits"] = gconf.count()
    if R["game_conflits"]:
        write_parquet(gconf, f"{Q}/games_conflicts")
    games = (game.withColumn("annee", F.col("season_id").cast("int") % 10000)
                 .withColumn("game_date", F.to_date("game_date")))

    aux_cols = {
        "line_score": ["game_id"] + [c for c in SCHEMAS["line_score"].fieldNames()
                                     if c.startswith("pts_qtr") or c.startswith("pts_ot")]
                      + ["ls_pts_home", "ls_pts_away"],
        "game_info": ["game_id", "attendance", "game_time"],
        "game_summary": ["game_id", "game_status_text", "season",
                         "natl_tv_broadcaster_abbreviation"],
        "other_stats": [c for c in SCHEMAS["other_stats"].fieldNames()
                        if c not in ("team_id_home", "team_id_away",
                                     "team_abbreviation_home", "team_abbreviation_away",
                                     "team_city_home", "team_city_away", "league_id")]}
    for name, cols in aux_cols.items():
        a = read_csv(spark, name)
        if name == "line_score":
            a = (a.withColumnRenamed("pts_home", "ls_pts_home")
                  .withColumnRenamed("pts_away", "ls_pts_away"))
        a = a.cache()
        n = a.count()
        d = a.dropDuplicates().cache()
        R[f"{name}_doublons_exact"] = n - d.count()
        a.unpersist()
        ok, conf = split_key_conflicts(d, ["game_id"])
        conf = conf.cache()
        R[f"{name}_conflits"] = conf.count()
        if R[f"{name}_conflits"]:
            write_parquet(conf, f"{Q}/{name}_conflicts")
        if name == "line_score":
            ok = align_home_away(ok, game, cols, R)
        games = games.join(F.broadcast(ok.select(*cols)), "game_id", "left")

    write_parquet(games.repartition(4), f"{OUT}/games")
    spark.catalog.clearCache()

    games = spark.read.parquet(f"{OUT}/games").cache()
    st = games.agg(
        F.count("*").alias("n"),
        F.sum(F.when(F.col("game_id").isNull(), 1).otherwise(0)).alias("cle_nulle"),
        F.sum(F.when(F.col("pts_home").isNotNull() & F.col("ls_pts_home").isNotNull()
                     & ((F.col("pts_home") != F.col("ls_pts_home"))
                        | (F.col("pts_away") != F.col("ls_pts_away"))), 1)
              .otherwise(0)).alias("ecarts_ls")).first()
    R["games_silver"] = st.n
    R["games_cle_nulle"] = st.cle_nulle
    R["pts_game_vs_linescore_ecarts"] = st.ecarts_ls
    return games


# play_by_play

def build_pbp(spark, games, R, sample=None, refresh=False):
    if refresh or not hdfs_exists(spark, f"{STAGING_PBP}/_SUCCESS"):
        print("conversion play_by_play.csv -> parquet typé (une seule lecture du CSV)")
        write_parquet(read_csv(spark, "play_by_play"), STAGING_PBP)
    pbp = spark.read.parquet(STAGING_PBP)

    if sample:
        keep = (games.filter(F.col("game_id").isNotNull())
                .orderBy(F.desc("game_date"), F.desc("game_id")).limit(sample)
                .select("game_id"))
        pbp = pbp.join(F.broadcast(keep), "game_id")
        R["mode"] = f"sample ({sample} matchs)"

    valid_cond = (F.col("game_id").isNotNull() & F.col("eventnum").isNotNull()
                  & F.col("period").isNotNull() & (F.col("period") >= 1)
                  & (F.col("period") <= PERIOD_MAX))
    st = pbp.agg(
        F.count("*").alias("n"),
        F.sum(F.when(F.col("game_id").isNull() | F.col("eventnum").isNull(), 1)
              .otherwise(0)).alias("cles_nulles"),
        F.sum(F.when(F.col("period").isNull() | (F.col("period") < 1)
                     | (F.col("period") > PERIOD_MAX), 1).otherwise(0)).alias("periode_inv"),
        F.sum(F.when(~F.coalesce(F.col("pctimestring").rlike("^[0-9]+:[0-9]{2}$"),
                                 F.lit(False)), 1).otherwise(0)).alias("horloge_inv"),
        F.sum(F.when(valid_cond, 1).otherwise(0)).alias("valides")).first()
    R["pbp_lignes"] = st.n
    R["pbp_cles_nulles"] = st.cles_nulles
    R["pbp_periodes_invalides"] = st.periode_inv
    R["pbp_horloges_invalides"] = st.horloge_inv

    # dédoublonnage exact, puis conflits de clé (game_id, eventnum), puis
    # rattachement à games (orphelins) -- tout en une passe matérialisée
    ga = games.select("game_id", "annee")
    d = (pbp.filter(valid_cond).dropDuplicates()
            .withColumn("_n", F.count(F.lit(1)).over(Window.partitionBy(*PBP_KEY)))
            .join(F.broadcast(ga), "game_id", "left")
            .persist(StorageLevel.DISK_ONLY))
    st2 = d.agg(
        F.count("*").alias("distinct"),
        F.sum(F.when(F.col("_n") > 1, 1).otherwise(0)).alias("conflits"),
        F.sum(F.when((F.col("_n") == 1) & F.col("annee").isNull(), 1)
              .otherwise(0)).alias("orphelins")).first()
    R["pbp_doublons_exact"] = st.valides - st2.distinct
    R["pbp_conflits"] = st2.conflits
    R["pbp_orphelins"] = st2.orphelins
    if R["pbp_conflits"]:
        write_parquet(d.filter("_n > 1").drop("_n"), f"{Q}/pbp_conflits")
    if R["pbp_orphelins"]:
        write_parquet(d.filter("_n = 1 AND annee IS NULL").drop("_n", "annee"),
                      f"{Q}/pbp_orphelins")

    clean = d.filter("_n = 1 AND annee IS NOT NULL").drop("_n")
    clean = parse_score(parse_clock(normalize_pbp_types(clean)))
    clean = clean.withColumn("sec",
        F.when(F.col("period") <= 4, (F.col("period") - 1) * 720 + 720 - F.col("rest"))
         .otherwise(2880 + (F.col("period") - 5) * 300 + 300 - F.col("rest")))
    write_parquet(clean.repartition("annee").sortWithinPartitions(*PBP_KEY),
                  f"{OUT}/play_by_play", part="annee")
    d.unpersist()

    pbp = spark.read.parquet(f"{OUT}/play_by_play")
    R["pbp_silver"] = pbp.count()

    # score final du play-by-play vs score officiel de game
    last = (pbp.filter("score_ok")
               .groupBy("game_id")
               .agg(F.max(F.struct("eventnum", "score_home", "score_away")).alias("l"))
               .select("game_id", "l.score_home", "l.score_away"))
    cmp_ = last.join(games.select("game_id", F.col("pts_home").alias("h"),
                                  F.col("pts_away").alias("a")), "game_id").cache()
    bad = cmp_.filter((F.col("score_home") != F.col("h")) | (F.col("score_away") != F.col("a")))
    R["score_final_matchs_verifies"] = cmp_.count()
    R["score_final_ecarts"] = bad.count()
    if R["score_final_ecarts"]:
        write_parquet(bad, f"{Q}/score_final_ecarts")
    cmp_.unpersist()

    return (pbp.groupBy("annee").agg(F.countDistinct("game_id").alias("matchs_pbp"),
                                     F.count("*").alias("evenements"))
               .orderBy("annee").collect())


def write_report(R, cov):
    L = ["# Contrôles Silver\n", "## État des données — Bronze → Silver\n"]
    evol = [("game + aux", "CSV HDFS /nba/raw", R["game_lignes"],
             R["games_silver"], "parquet HDFS /nba/silver/games",
             f"{R['game_doublons_exact']} doublons exacts, {R['game_conflits']} conflits -> quarantaine"),
            ("play_by_play", "CSV HDFS /nba/raw", R["pbp_lignes"],
             R["pbp_silver"], "parquet HDFS /nba/silver/play_by_play (partition annee)",
             f"{R['pbp_doublons_exact']} doublons exacts, {R['pbp_conflits']} conflits, "
             f"{R['pbp_orphelins']} orphelins, {R['pbp_cles_nulles']} cles nulles, "
             f"{R['pbp_periodes_invalides']} periodes invalides")]
    L.append(md_table(["table", "bronze", "lignes bronze", "lignes silver", "sortie", "écarts"],
                      evol))
    L.append("\n## Comptes\n")
    L.append(md_table(["contrôle", "valeur"],
                      [(k, f"{v:,}".replace(",", " ") if isinstance(v, (int, float)) else v)
                       for k, v in R.items()]))
    L.append("\n## Couverture play-by-play par saison (silver)\n")
    L.append(md_table(["saison", "matchs", "événements"],
                      [(r.annee, r.matchs_pbp, r.evenements) for r in cov]))
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w", encoding="utf-8") as f:
        f.write("\n".join(L))


def main(sample=None, refresh=False, skip_pbp=False):
    spark = (SparkSession.builder
             .appName("nba-silver")
             .master("spark://spark-master:7077")
             .config("spark.executor.cores", "2")
             .config("spark.executor.memory", "2g")
             .config("spark.sql.shuffle.partitions", "48")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    R = {}
    games = build_games(spark, R)
    print(f"games ok : {R['games_silver']} matchs")
    if not skip_pbp:
        cov = build_pbp(spark, games, R, sample=sample, refresh=refresh)
        write_report(R, cov)
    for k, v in R.items():
        print(f"{k}: {v}")
    print("play_by_play non reconstruit (--skip-pbp), rapport inchangé" if skip_pbp
          else f"rapport : {REPORT}")
    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=None,
                    help="limiter aux N matchs les plus recents (test rapide)")
    ap.add_argument("--refresh", action="store_true",
                    help="reconvertir play_by_play.csv en parquet meme si le staging existe")
    ap.add_argument("--skip-pbp", action="store_true",
                    help="ne reconstruire que la table games (play_by_play inchangé)")
    args = ap.parse_args()
    main(args.sample, args.refresh, args.skip_pbp)
