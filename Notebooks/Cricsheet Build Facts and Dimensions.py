#!/usr/bin/env python
# coding: utf-8

# ## Cricsheet Build Facts and Dimensions
# 
# New notebook

# # Initialize Common Functions and Libraries

# In[ ]:


get_ipython().run_line_magic('run', '"/Common Functions"')


# # Set Variables

# In[ ]:


raw_df = read_delta_table(raw_table_path, "t_cricsheet")
team_player_schema = T.MapType(T.StringType(), T.ArrayType(T.StringType()))
array_schema  = T.ArrayType(T.StringType())


# # Check if Fact table row count is same as Raw table

# In[ ]:


match_table_to_check = [clean_table_path, "t_dim_match"]
if delta_table_exists(*match_table_to_check):
    row_count_match = read_delta_table(*match_table_to_check).count()

    row_count_raw = raw_df.count()

    if(row_count_match == row_count_raw):
        mssparkutils.notebook.exit(1)
    else:
        print("Cricsheet has " + str(row_count_raw - row_count_fact) + " more matches added" )


# # Create Team Player Dataframe and Cache it

# In[ ]:


tpd = (
    raw_df
    .select("match_id", 
            F.explode(F.from_json(F.get_json_object("match_info",'$.players'), team_player_schema)))
    .select("match_id",
            F.col("key").alias("team"),
            F.explode("value").alias("player_name"))
    .cache()
)
tpd = tpd.alias("tpd")


# # Create t_dim_player

# In[ ]:


pdf = (
    tpd
    .select("player_name")
    .distinct()
    .select(F.monotonically_increasing_id().alias("player_id"), "*")
    .union(spark.createDataFrame([[-1, "Extras"]]))
)

create_or_replace_delta_table(pdf, clean_table_path, "t_dim_player")


# # Create t_dim_team

# In[ ]:


tdf = (
    tpd
    .select("team")
    .distinct()
    .select(F.monotonically_increasing_id().alias("team_id"), "*")
)

create_or_replace_delta_table(tdf, clean_table_path, "t_dim_team")


# # Create Dataframe aliases for join purpose

# In[ ]:


pdf = read_delta_table(clean_table_path, "t_dim_player").alias("pdf")
pom = pdf.alias("pom")
bat = pdf.alias("bat")
bwl = pdf.alias("bwl")
fld = pdf.alias("fld")
nsr = pdf.alias("nsr")
pot = pdf.alias("pot")
tdf = read_delta_table(clean_table_path, "t_dim_team").alias("tdf")
mwn = tdf.alias("mwn")
elm = tdf.alias("elm")
twn = tdf.alias("twn")
fbt = tdf.alias("fbt")
flt = tdf.alias("flt")


# # Create t_dim_match

# In[ ]:


mdf_json_fields = [
    ["dates[0]", "date", "match_date"],
    ["gender", "string", "match_gender"],
    ["season", "string", "season"],
    ["event.name", "string", "event_name"],
    ["event.group", "string", "event_group"],
    ["event.match_number", "int", "event_match_number"],
    ["city", "string", "city"],
    ["venue", "string", "venue"],
    ["officials.umpires", "string", "umpires"],
    ["team_type","string", "team_type"],
    ["match_type", "string", "match_type"],
    ["outcome.winner", "string", "match_winner"],
    ["outcome.result", "string", "match_result"],
    ["outcome.by.runs", "string", "match_won_by_runs"],
    ["outcome.by.wickets", "string", "match_won_by_wickets"],
    ["outcome.by.innings", "string", "match_won_by_innings"],
    ["outcome.eliminator", "string", "match_winner_eliminator"],
    ["outcome.method", "string", "match_result_method"],
    ["toss.winner", "string", "toss_winner"],
    ["toss.decision", "string", "toss_decision"],
    ["player_of_match[0]", "string", "player_of_match"],
    ["players", "string", "team_players"]
]
mdf_json_select_lst = [
    F.get_json_object("match_info", "$." + json[0]).cast(json[1]).alias(json[2])
    for json in mdf_json_fields
]
mdf = (
  raw_df
  .select("match_id",
          *mdf_json_select_lst)
  .select("*", 
          first_team('bat').alias("first_bat"),
          first_team('field').alias("first_field")))
mdf = mdf.alias("mdf")
mdf = ( 
  mdf
  .join(twn, twn.team == mdf.toss_winner, 'leftouter' )
  .join(mwn, mwn.team == mdf.match_winner, 'leftouter' )
  .join(elm, elm.team == mdf.match_winner_eliminator, 'leftouter' )
  .join(pom, pom.player_name == mdf.player_of_match, 'leftouter' )
  .join(fbt, fbt.team == mdf.first_bat, 'leftouter' )
  .join(flt, flt.team == mdf.first_field, 'leftouter' )
  .select("mdf.match_id",
          "mdf.match_date",
          "mdf.match_gender",
          "mdf.season",
          "mdf.event_name",
          "mdf.event_group",
          "mdf.event_match_number",
          "mdf.city",
          "mdf.venue",
          "mdf.team_type",
          "mdf.match_type",
          "mdf.match_result",
          "mdf.match_won_by_runs",
          "mdf.match_won_by_wickets",
          "mdf.match_won_by_innings",
          "mdf.match_result_method",
          F.concat_ws(", ", F.from_json("umpires", array_schema)).alias("umpires"),
          F.coalesce("mwn.team_id", "elm.team_id").alias("match_winner_id"),
          F.col("twn.team_id").alias("toss_winner_id"),
          "mdf.toss_decision",
          F.col("pom.player_id").alias("player_of_match_id"),
          F.col("fbt.team_id").alias("first_bat_id"),
          F.col("flt.team_id").alias("first_field_id"))
)

create_or_replace_delta_table(mdf, clean_table_path, "t_dim_match")

mdf = read_delta_table(clean_table_path, "t_dim_match").alias("mdf")


# # Create t_dim_date

# In[ ]:


dte = (
    read_delta_table(clean_table_path, "t_dim_match")
    .select(F.trunc(F.min("match_date"), "YY").alias("start_date"),
            F.add_months(F.trunc(F.max("match_date"), "YY")-1,12).alias("end_date"))
    .select(F.explode(F.sequence("start_date", "end_date")).alias("date"))
    .select("date",
            F.year("date").alias("year"),
            F.concat(F.lit("Q"), F.quarter("date")).alias("quarter"),
            F.month("date").alias("month_number"),
            F.date_format("date", "MMM").alias("month"))
)

create_or_replace_delta_table(dte, clean_table_path, "t_dim_date")


# # Create t_fact_team_players

# In[ ]:


tpl = (
  tpd
  .join(tdf, tdf.team == tpd.team, 'inner' )
  .join(pdf, pdf.player_name == tpd.player_name, 'inner' )
  .join(mdf, mdf.match_id == tpd.match_id, 'inner')
  .select("tpd.match_id", 
          "tdf.team_id", 
          "pdf.player_id",
          "mdf.match_date",
          "mdf.match_winner_id",
          "mdf.toss_winner_id",
          "mdf.player_of_match_id",
          "mdf.first_bat_id",
          "mdf.first_field_id")
)

create_or_replace_delta_table(tpl, clean_table_path, "t_fact_team_players")


# # Create t_fact_deliveries

# In[ ]:


dlv_json_fields = [
    ["batter", "string", "batter_name"],
    ["bowler", "string", "bowler_name"],
    ["extras.byes", "int", "byes"],
    ["extras.legbyes", "int", "leg_byes"],
    ["extras.noballs", "int", "no_balls"],
    ["extras.wides", "int", "wides"],
    ["runs.batter", "int", "batter_runs"],
    ["runs.total", "int", "total_runs"],
    ["non_striker", "string", "non_striker_name"],
    ["wickets[0].kind", "string", "wicket_kind"],
    ["wickets[0].player_out", "string", "player_out"],
    ["wickets[0].fielders[0].name", "string", "fielder_name"]
]
dlv_json_select_lst = [
    F.get_json_object("col", "$." + json[0]).cast(json[1]).alias(json[2])
    for json in dlv_json_fields
]
dlv = (
  raw_df
  .repartition(200)
  .select("match_id",
          F.posexplode(F.from_json("match_innings", array_schema)))
  .select("match_id",
          (F.col("pos") + 1).alias("innings"),
          F.get_json_object("col",'$.team').alias("team"),
          F.posexplode(F.from_json(F.get_json_object("col",'$.overs'), array_schema)))
  .select("match_id",
          "innings",
          "team",
          (F.col("pos") + 1).alias("overs"),
          F.posexplode(F.from_json(F.get_json_object("col",'$.deliveries'), array_schema)))
  .select("*",
          *dlv_json_select_lst)
)

dlv = dlv.alias("dlv")
window_spec = Window.partitionBy("match_id").orderBy("team_id")
tpl = (
  read_delta_table(clean_table_path, "t_fact_team_players")
  .select("match_id", "team_id")
  .distinct()
  .withColumn("next_team_id", F.coalesce(F.lead("team_id", 1).over(window_spec), F.lag("team_id", 1).over(window_spec)))
  .alias("tpl")
)

dlv = (
        dlv
        .join(mdf, mdf.match_id == dlv.match_id, 'inner')
        .join(tdf, tdf.team == dlv.team, 'inner')
        .join(tpl, [tpl.team_id == tdf.team_id, tpl.match_id == dlv.match_id], 'inner')
        .join(bat, bat.player_name == dlv.batter_name, 'leftouter')
        .join(bwl, bwl.player_name == dlv.bowler_name, 'leftouter')
        .join(fld, fld.player_name == dlv.fielder_name, 'leftouter')
        .join(pot, pot.player_name == dlv.player_out, 'leftouter')
        .join(nsr, nsr.player_name == dlv.non_striker_name, 'leftouter')
        .select("dlv.match_id",
                "mdf.match_date",
                F.col("tdf.team_id").alias("batting_team_id"),
                F.col("tpl.next_team_id").alias("bowling_team_id"),
                "dlv.innings",
                "dlv.overs",
                (F.col("dlv.pos") + 1).alias("balls"),
                F.col("bat.player_id").alias("batter_id"),
                F.col("nsr.player_id").alias("non_striker_id"),
                F.col("bwl.player_id").alias("bowler_id"),
                "dlv.byes",
                "dlv.leg_byes",
                "dlv.no_balls",
                "dlv.wides",
                "dlv.batter_runs",
                "dlv.total_runs",
                "dlv.wicket_kind",
                F.col("pot.player_id").alias("player_out_id"),
                F.col("fld.player_id").alias("fielder_id"))   
)

create_or_replace_delta_table(dlv, clean_table_path, "t_fact_deliveries")

