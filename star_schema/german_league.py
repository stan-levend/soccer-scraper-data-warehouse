import time
from db_manager import DatabaseManager
from query_strings import german_events, german_linups, german_subs

star_schema_manager = DatabaseManager('postgres', 'postgres', 'star_schema')


def fill_in_german_league(league_manager):

    fill_in_event_fact_table(league_manager)
    fill_in_lineup_fact_table(league_manager)

    league_manager.close_connection()
    star_schema_manager.close_connection()


def insert_into_common_tables(league_manager, name, position, birth_date, nationality, match_date, season, result, home_team, playing_team, venue):
    player_id = star_schema_manager.insert_into_table("player", {
        "name": name.replace("'", " "),
        "position": position,
        "birth_year": str(birth_date.year),
        "nationality": nationality.replace("'", " ")
    })

    match_date_id = star_schema_manager.insert_into_table("match_date", {
        "season": f"20{str(season[0])}-20{str(season[1])}",
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
        "league_name": "Bundesliga",
        "team_name": playing_team
    })

    return player_id, match_date_id, match_id, league_team_id


def fill_in_lineup_fact_table(league_manager):
    lineup_records = league_manager.query(german_linups)

    print(f"Processing german lineups - {len(lineup_records)} records.")
    for i, record in enumerate(lineup_records[:]):
        # print(record)
        split_string_season = record[8].split('/')
        result = league_manager.get_result_from_goals(record[9], record[10])

        player_id, match_date_id, match_id, league_team_id = insert_into_common_tables(league_manager,
            name=record[0], position=record[1], birth_date=record[2], nationality=record[3], match_date=record[7], season=split_string_season,
            result=result, home_team=record[4], playing_team=record[6], venue=record[11]
        )

        fact_table_fk_dict = {"player_id": str(player_id), "match_date_id": str(match_date_id), "match_id": str(match_id), "league_team_id": str(league_team_id)}
        fact_table_facts_dict = {"time_played": str(90)}
        fact_table_content = dict(fact_table_fk_dict, **fact_table_facts_dict)

        fetched_record = star_schema_manager.fact_event_table_record_exists("lineup_fact_table", fact_table_fk_dict)
        if not fetched_record:
            match_date_id = star_schema_manager.insert_into_table("lineup_fact_table", fact_table_content, id=None)
        else:
            # print(f"{i} Duplicate player, not inserting.")
            pass

        # if i % 1000 == 0: print(f"{i} records done")


    substitution_records = league_manager.query(german_subs)
    print(f"Processing german substitution_records for lineups - {len(substitution_records)} records.")
    for i, record in enumerate(substitution_records[:]):
        # print(record)
        split_string_season = record[8].split('/')
        result = league_manager.get_result_from_goals(record[9], record[10])

        player_id, match_date_id, match_id, league_team_id = insert_into_common_tables(league_manager,
            name=record[0], position=record[1], birth_date=record[2], nationality=record[3], match_date=record[7], season=split_string_season,
            result=result, home_team=record[4], playing_team=record[6], venue=record[13]
        )

        fact_table_fk_dict = {"player_id": str(player_id), "match_date_id": str(match_date_id), "match_id": str(match_id), "league_team_id": str(league_team_id)}
        event_time = record[12]

        fetched_record = star_schema_manager.fact_event_table_record_exists("lineup_fact_table", fact_table_fk_dict)
        if fetched_record is not None:
            if event_time > 90: new_time = 90
            else: new_time = event_time
            star_schema_manager.update_record("lineup_fact_table", fact_table_fk_dict, "time_played", new_time)

        if record[14] is not None:
            player = league_manager.get_player_info_by_player_id(record[14])
            if player is not None:
                # print(player)
                player_id, match_date_id, match_id, league_team_id = insert_into_common_tables(league_manager,
                    name=player[0], position=player[1], birth_date=player[2], nationality=player[3], match_date=record[7], season=split_string_season,
                    result=result, home_team=record[4], playing_team=record[6], venue=record[13]
                )
                if event_time > 90: new_time = event_time - 90
                else: new_time = 90 - event_time

                fact_table_fk_dict = { "player_id": str(player_id), "match_date_id": str(match_date_id), "match_id": str(match_id), "league_team_id": str(league_team_id) }
                fact_table_facts_dict = { "time_played": str(new_time) }
                fact_table_content = dict(fact_table_fk_dict, **fact_table_facts_dict)

                fetched_record = star_schema_manager.fact_event_table_record_exists("lineup_fact_table", fact_table_fk_dict)
                if not fetched_record:
                    match_date_id = star_schema_manager.insert_into_table("lineup_fact_table", fact_table_content, id=None)
                else:
                    # print(f"{i} Already in the fact table")
                    star_schema_manager.update_record("lineup_fact_table", fact_table_fk_dict, "time_played", new_time)

        # if i % 1000 == 0: print(f"{i} records done")


def fill_in_event_fact_table(league_manager):
    records = league_manager.query(german_events)

    print(f"Processing german events - {len(records)} records.")
    for i, record in enumerate(records[:]):
        # print(record)
        split_string_season = record[8].split('/')
        result = league_manager.get_result_from_goals(record[9], record[10])

        player_id, match_date_id, match_id, league_team_id = insert_into_common_tables(league_manager,
            name=record[0], position=record[1], birth_date=record[2], nationality=record[3], match_date=record[7], season=split_string_season,
            result=result, home_team=record[4], playing_team=record[6], venue=record[14]
        )

        event_time_id = star_schema_manager.insert_into_table("event_time", {
            "time": str(record[12]),
            "half": str(league_manager.get_half_from_minute(record[12]))
        })

        fact_table_fk_dict = {
            "player_id": str(player_id), "match_date_id": str(match_date_id), "match_id": str(match_id), "league_team_id": str(league_team_id), "event_time_id": str(event_time_id),
        }

        event_type = record[11]
        fact_table_facts_dict = {
            "count_goals": str(1 if event_type == "goal" and record[13] is False else 0),
            "count_own_goals": str(1 if event_type == "goal" and record[13] is True else 0),
            "count_yellow_cards": str(1 if event_type == "yellowcard" or event_type == "yellowredcard" else 0),
            "count_red_cards": str(1 if event_type == "redcard" else 0),
            "count_assists": str(0),
            "count_substitution_off": str(1 if event_type == "substitution" else 0),
            "count_substitution_in": str(0)
        }

        fact_table_content = dict(fact_table_fk_dict, **fact_table_facts_dict)

        fetched_record = star_schema_manager.fact_event_table_record_exists("event_fact_table", fact_table_fk_dict)
        if not fetched_record:
            match_date_id = star_schema_manager.insert_into_table("event_fact_table", fact_table_content, id=None)
        else:
            attribute_to_inc = star_schema_manager.get_dict_key_for_increment(fact_table_facts_dict, 1)
            # print(f"{i} Already in the fact table")
            star_schema_manager.increment_attribute_in_record("event_fact_table", fact_table_fk_dict, attribute_to_inc)

        if record[15]: #If related player exists -> get info about him and fill in the event
            player = league_manager.get_player_info_by_player_id(record[15])
            if player is not None:
                player_id, match_date_id, match_id, league_team_id = insert_into_common_tables(league_manager,
                    name=player[0], position=player[1], birth_date=player[2], nationality=player[3], match_date=record[7], season=split_string_season,
                    result=result, home_team=record[4], playing_team=record[6], venue=record[14]
                )
                fact_table_fk_dict = { "player_id": str(player_id), "match_date_id": str(match_date_id), "match_id": str(match_id), "league_team_id": str(league_team_id), "event_time_id": str(event_time_id) }
                fact_table_facts_dict = { "count_goals": str(0), "count_own_goals": str(0), "count_yellow_cards": str(0), "count_red_cards": str(0), "count_assists": str(0), "count_substitution_in": str(1), "count_substitution_off": str(0) }
                fact_table_content = dict(fact_table_fk_dict, **fact_table_facts_dict)

                fetched_record = star_schema_manager.fact_event_table_record_exists("event_fact_table", fact_table_fk_dict)
                if not fetched_record:
                    match_date_id = star_schema_manager.insert_into_table("event_fact_table", fact_table_content, id=None)
                else:
                    attribute_to_inc = star_schema_manager.get_dict_key_for_increment(fact_table_facts_dict, 1)
                    # print(f"{i} Already in the fact table")
                    star_schema_manager.increment_attribute_in_record("event_fact_table", fact_table_fk_dict, attribute_to_inc)

        # if i % 1000 == 0: print(f"{i} records done")



if __name__ == '__main__':
    # fill_in_german_league()
    pass