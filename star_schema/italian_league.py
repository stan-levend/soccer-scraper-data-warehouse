import time
from db_manager import DatabaseManager
from query_strings import italian_events, italian_subs, italian_lineups

star_schema_manager = DatabaseManager('postgres', 'postgres', 'star_schema')
league_manager = DatabaseManager('postgres', 'postgres', 'tassu')

def main():
    # index = manager.get_index_if_exists('match', {'date': '2018-08-18', 'home_goals': 2, 'away_goals': 3, 'season': 2018})
    # index = manager.insert_into_table('building', {'city': 'kosice', 'name': 'test', 'street': 'sturova 10'})

    # fill_in_event_fact_table()
    fill_in_lineup_fact_table()

    star_schema_manager.close_connection()

def insert_into_common_tables(name, position, birth_date, nationality, match_date, season, result, home_team, playing_team):
    player_id = star_schema_manager.insert_into_table("player", {
        "name": name.replace("'", " "),
        "position": position,
        "birth_year": str(birth_date.year),
        "nationality": nationality.replace("'", " ")
    })

    match_date_id = star_schema_manager.insert_into_table("match_date", {
        "season": f"{str(season)}-{str(season+1)}",
        "year": str(match_date.year),
        "month": match_date.strftime("%B"),
        "day": str(match_date.day),
        "quarter": str(league_manager.get_quarter_from_date(match_date)),
        "day_of_week": league_manager.get_day_of_week_from_date(match_date),
        "weekend": str(league_manager.get_weekend_flag_from_date(match_date))
    })

    match_id = star_schema_manager.insert_into_table("match", {
        "venue": league_manager.get_italian_venue_from_team(home_team),
        "result": league_manager.get_result_from_char(result),
        "team": "Home" if home_team == playing_team else "Away"
    })

    league_team_id = star_schema_manager.insert_into_table("league_team", {
        "league_name": "Serie A",
        "team_name": playing_team
    })

    return player_id, match_date_id, match_id, league_team_id


def fill_in_lineup_fact_table():
    lineup_records = league_manager.query(italian_lineups)

    print(f"Processing {len(lineup_records)}.")
    for i, record in enumerate(lineup_records[:]):

        player_id, match_date_id, match_id, league_team_id = insert_into_common_tables(
            name=record[0], position=record[1], birth_date=record[2], nationality=record[3], match_date=record[7], season=record[8],
            result=record[9], home_team=record[4], playing_team=record[6]
        )
        is_substitute = record[10]

        fact_table_fk_dict = {"player_id": str(player_id), "match_date_id": str(match_date_id), "match_id": str(match_id), "league_team_id": str(league_team_id)}
        fact_table_facts_dict = {"time_played": str(0 if is_substitute else 90)}
        fact_table_content = dict(fact_table_fk_dict, **fact_table_facts_dict)

        fetched_record = star_schema_manager.fact_event_table_record_exists("lineup_fact_table", fact_table_fk_dict)
        if not fetched_record:
            match_date_id = star_schema_manager.insert_into_table("lineup_fact_table", fact_table_content, id=None)
        else:
            print(f"{i} Duplicate player, not inserting.")

        if i % 1000 == 0: print(f"{i} records done")


    # WE NEED TO FIND SUBSTITUTIONS AND SUBSTRACT/ADD TIME TO THE TIME PLAYED THAT WE HAVE OBTAINED ALREADY
    substitution_records = league_manager.query(italian_subs)
    print(f"Processing {len(substitution_records)} substitution_records.")
    for i, record in enumerate(substitution_records[:]):
        player_id, match_date_id, match_id, league_team_id = insert_into_common_tables(
            name=record[0], position=record[1], birth_date=record[2], nationality=record[3], match_date=record[7], season=record[8],
            result=record[9], home_team=record[4], playing_team=record[6]
        )
        is_substitute = record[10]
        # print(i, record)
        fact_table_fk_dict = {"player_id": str(player_id), "match_date_id": str(match_date_id), "match_id": str(match_id), "league_team_id": str(league_team_id)}

        event_type = record[11]
        event_time = league_manager.get_minute_from_string(record[12])

        fetched_record = star_schema_manager.fact_event_table_record_exists("lineup_fact_table", fact_table_fk_dict)
        if fetched_record:
            if event_time > 90:
                if event_type == 'sub on': new_time = event_time - 90
                elif event_type == 'sub off': new_time = 90
            else:
                if event_type == 'sub on': new_time = 90 - event_time
                elif event_type == 'sub off': new_time = event_time
            star_schema_manager.update_record("lineup_fact_table", fact_table_fk_dict, "time_played", new_time)
        else:
            print("Record not found")

        if i % 1000 == 0: print(f"{i} records done")


def fill_in_event_fact_table():
    records = league_manager.query(italian_events)

    print(f"Processing {len(records)}.")
    for i, record in enumerate(records[:5]):
        # print(record)
        player_id, match_date_id, match_id, league_team_id = insert_into_common_tables(
            name=record[0], position=record[1], birth_date=record[2], nationality=record[3], match_date=record[7], season=record[8],
            result=record[9], home_team=record[4], playing_team=record[6]
        )

        minute = league_manager.get_minute_from_string(record[12]),
        event_time_id = star_schema_manager.insert_into_table("event_time", {
            "time": str(minute[0]),
            "half": str(league_manager.get_half_from_minute(minute[0]))
        })

        fact_table_fk_dict = {
            "player_id": str(player_id), "match_date_id": str(match_date_id), "match_id": str(match_id), "league_team_id": str(league_team_id), "event_time_id": str(event_time_id),
        }

        event_type = record[11]
        fact_table_facts_dict = {
            "count_goals": str(1 if event_type == "goal" else 0),
            "count_own_goals": str(1 if event_type == "own goal" else 0),
            "count_yellow_cards": str(1 if event_type == "yellow card" else 0),
            "count_red_cards": str(1 if event_type == "red card" else 0),
            "count_assists": str(1 if event_type == "assist" else 0),
            "count_substitution_in": str(1 if event_type == "sub on" else 0),
            "count_substitution_off": str(1 if event_type == "sub off" else 0)
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