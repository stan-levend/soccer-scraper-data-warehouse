import time
import pandas as pd
import numpy as np

from datetime import date
from datetime import datetime
from psycopg2 import Error

from db_manager import DatabaseManager
from query import dutch_event, dutch_lineup

star_schema_manager = DatabaseManager('postgres', 'postgres', 'star_schema')
league_manager = DatabaseManager('postgres', 'postgres', 'tassu-holandska_liga')

countries_df = pd.read_csv(f"matches.csv", usecols=['Date', 'HomeTeam', 'AwayTeam', 'FTR'])
def get_result_by_code(code1, code2, code3):
    try: result = countries_df.loc[(countries_df['Date'] == str(code1)) & (countries_df['HomeTeam'] == str(code2)) & (countries_df['AwayTeam'] == str(code3)), 'FTR'].iloc[0]
    except: result=None
    #except: raise ValueError(f'{code1} {code2} {code3} result problem')
    return result
    print(result)


def main():
    # index = manager.get_index_if_exists('match', {'date': '2018-08-18', 'home_goals': 2, 'away_goals': 3, 'season': 2018})
    # index = manager.insert_into_table('building', {'city': 'kosice', 'name': 'test', 'street': 'sturova 10'})

    # fill_in_event_fact_table()
    fill_in_lineup_fact_table()

    star_schema_manager.close_connection()

def insert_into_common_tables(name, position, birth_date, nationality, match_date, season, result, home_team, playing_team, venue):
    player_id = star_schema_manager.insert_into_table("player", {
        "name": name.replace("'", " "),
        "position": position,
        "birth_year": str(birth_date.year),
        # "birth_month": birth_date.strftime("%B"),
        "nationality": nationality.replace("'", " ")
    })

    match_date_id = star_schema_manager.insert_into_table("match_date", {
        "season": str(season),
        "year": str(match_date.year),
        "month": match_date.strftime("%B"),
        "day": str(match_date.day),
        "quarter": str(league_manager.get_quarter_from_date(match_date)),
        "day_of_week": league_manager.get_day_of_week_from_date(match_date),
        "weekend": str(league_manager.get_weekend_flag_from_date(match_date))
    })

    match_id = star_schema_manager.insert_into_table("match", {
        "venue": str(venue),
        "result": str(result),
        "team": "Home" if home_team == playing_team else "Away"
    })

    league_team_id = star_schema_manager.insert_into_table("league_team", {
        "league_name": "Eredivisie",
        "team_name": playing_team
    })

    return player_id, match_date_id, match_id, league_team_id


def fill_in_lineup_fact_table():
    lineup_records = league_manager.query(dutch_lineup)


    print(f"Processing {len(lineup_records)}.")
    for i, record in enumerate(lineup_records[:]):
        result = get_result_by_code(record[8], record[5], record[6])
        if(result!=None):
            result = league_manager.get_result_from_char(result)
        else:
            i+=1

        player_id, match_date_id, match_id, league_team_id = insert_into_common_tables(
            name=record[0], position=record[1], birth_date=record[2], nationality=record[3], match_date=record[8], season=record[9],
            result=result, home_team=record[5], playing_team=record[7], venue=(record[10])
        )
        minutes=record[11]

        fact_table_fk_dict = {"player_id": str(player_id), "match_date_id": str(match_date_id), "match_id": str(match_id), "league_team_id": str(league_team_id)}
        fact_table_facts_dict = {"time_played": str(minutes)}
        fact_table_content = dict(fact_table_fk_dict, **fact_table_facts_dict)

        fetched_record = star_schema_manager.fact_event_table_record_exists("lineup_fact_table", fact_table_fk_dict)
        if not fetched_record:
            match_date_id = star_schema_manager.insert_into_table("lineup_fact_table", fact_table_content, id=None)
        else:
            print(f"{i} Duplicate player, not inserting.")

        if i % 1000 == 0: print(f"{i} records done")


def fill_in_event_fact_table():
    records = league_manager.query(dutch_event)

    print(f"Processing {len(records)}.")
    for i, record in enumerate(records[:5]):
        # print(record)
        result = get_result_by_code(record[4], record[7], record[8])
        if(result!=None):
            result = league_manager.get_result_from_char(result)
        else:
            i+=1

        player_id, match_date_id, match_id, league_team_id = insert_into_common_tables(
            name=record[0], position=record[1], birth_date=record[2], nationality=record[3], match_date=record[4], season=record[5],
            result=result, home_team=record[7], playing_team=record[9], venue=record[10]
        )

        minute = record[12],
        half = record[13]
        event_time_id = star_schema_manager.insert_into_table("event_time", {
            "time": str(minute),
            "half": str(league_manager.get_half_from_minute(minute[0]))
        })

        fact_table_fk_dict = {
            "player_id": str(player_id), "match_date_id": str(match_date_id), "match_id": str(match_id), "league_team_id": str(league_team_id), "event_time_id": str(event_time_id),
        }

        event_type = record[11]
        fact_table_facts_dict = {
            "count_goals": str(1 if event_type == "goal" else 0),
            "count_own_goals": str(1 if event_type == "own_goal" else 0),
            "count_yellow_cards": str(1 if event_type == "yellow_card" else 0),
            "count_red_cards": str(1 if event_type == "red_card" else 0),
            "count_assists": str(1 if event_type == "assist" else 0),
            "count_substitution_in": str(1 if event_type == "substitution_in" else 0),
            "count_substitution_off": str(1 if event_type == "substitution_out" else 0)
        }

        fact_table_content = dict(fact_table_fk_dict, **fact_table_facts_dict)

        fetched_record = star_schema_manager.fact_event_table_record_exists("event_fact_table", fact_table_fk_dict)
        if not fetched_record:
            match_date_id = star_schema_manager.insert_into_table("event_fact_table", fact_table_content, id=None)
        else:
            attribute_to_inc = star_schema_manager.get_dict_key_for_increment(fact_table_facts_dict, 1)
            print(f"{i} Already in the fact table")
            star_schema_manager.increment_attribute_in_record("event_fact_table", fact_table_fk_dict, attribute_to_inc)
        if i % 1000 == 0: print(f"{i} records done")

    league_manager.close_connection()


if __name__ == '__main__':
    main()
    pass



