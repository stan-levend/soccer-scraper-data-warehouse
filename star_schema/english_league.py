import time
from db_manager import DatabaseManager
from query_strings import english_substitutions, english_goals, english_cards, english_assists
from db_connector import star_schema_manager


def fill_in_english_league(league_manager):

    fill_in_event_fact_table_cards(league_manager)
    fill_in_event_fact_table_goals(league_manager)
    fill_in_event_fact_table_substitutions(league_manager)
    fill_in_event_fact_table_assists(league_manager)

    league_manager.close_connection()

def insert_into_common_tables(league_manager, name, position, birth_date, nationality, match_date, season, result, home_team, playing_team, venue):
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
        "venue": venue.replace("'", " "),
        "result": result,
        "team": "Home" if home_team == playing_team else "Away"
    })

    league_team_id = star_schema_manager.insert_into_table("league_team", {
        "league_name": "Premier League",
        "team_name": playing_team
    })

    return player_id, match_date_id, match_id, league_team_id


def fill_in_event_fact_table_cards(league_manager):
    records = league_manager.query(english_cards)

    print(f"Processing english cards - {len(records)} records.")
    for i, record in enumerate(records[:]):
        # print(record)

        venue_city = record[5].split('-')
        result = league_manager.get_result_from_goals(record[6], record[7])
        season = f"{record[8].year}-{record[9].year}"

        player_id, match_date_id, match_id, league_team_id = insert_into_common_tables(league_manager,
            name=record[0].strip(), position=record[3].strip(), birth_date=record[2], nationality=record[1].strip(), match_date=record[4], season=season,
            result=result, home_team=record[11], playing_team=record[10], venue=venue_city[0]
        )
        event_time_id = star_schema_manager.insert_into_table("event_time", {
            "time": str(record[14]),
            "half": str(league_manager.get_half_from_minute(record[14]))
        })

        fact_table_fk_dict = { "player_id": str(player_id), "match_date_id": str(match_date_id), "match_id": str(match_id), "league_team_id": str(league_team_id), "event_time_id": str(event_time_id), }

        event_type = record[13].strip()
        # print(event_type)
        fact_table_facts_dict = {
            "count_goals": str(0),
            "count_own_goals": str(0),
            "count_yellow_cards": str(1 if event_type == "yellow" else 0),
            "count_red_cards": str(1 if event_type == "red" else 0),
            "count_assists": str(0),
            "count_substitution_in": str(0),
            "count_substitution_off": str(0)
        }
        # print(fact_table_facts_dict)
        fact_table_content = dict(fact_table_fk_dict, **fact_table_facts_dict)

        fetched_record = star_schema_manager.fact_event_table_record_exists("event_fact_table", fact_table_fk_dict)
        if not fetched_record:
            match_date_id = star_schema_manager.insert_into_table("event_fact_table", fact_table_content, id=None)
        else:
            attribute_to_inc = star_schema_manager.get_dict_key_for_increment(fact_table_facts_dict, 1)
            # print(f"{i} Already in the fact table")
            star_schema_manager.increment_attribute_in_record("event_fact_table", fact_table_fk_dict, attribute_to_inc)
        # if i % 1000 == 0: print(f"{i} records done")


def fill_in_event_fact_table_goals(league_manager):
    records = league_manager.query(english_goals)

    print(f"Processing english goals - {len(records)} records.")
    for i, record in enumerate(records[:]):
        # print(record)

        venue_city = record[5].split('-')
        result = league_manager.get_result_from_goals(record[6], record[7])
        season = f"{record[8].year}-{record[9].year}"

        player_id, match_date_id, match_id, league_team_id = insert_into_common_tables(league_manager,
            name=record[0].strip(), position=record[3].strip(), birth_date=record[2], nationality=record[1].strip(), match_date=record[4], season=season,
            result=result, home_team=record[11], playing_team=record[10], venue=venue_city[0]
        )
        event_time_id = star_schema_manager.insert_into_table("event_time", {
            "time": str(record[13]),
            "half": str(league_manager.get_half_from_minute(record[13]))
        })

        fact_table_fk_dict = {"player_id": str(player_id), "match_date_id": str(match_date_id), "match_id": str(match_id), "league_team_id": str(league_team_id), "event_time_id": str(event_time_id), }
        fact_table_facts_dict = {"count_goals": str(1), "count_own_goals": str(0), "count_yellow_cards": str(0), "count_red_cards": str(0), "count_assists": str(0), "count_substitution_in": str(0), "count_substitution_off": str(0)}
        fact_table_content = dict(fact_table_fk_dict, **fact_table_facts_dict)
        # print(fact_table_facts_dict)

        fetched_record = star_schema_manager.fact_event_table_record_exists("event_fact_table", fact_table_fk_dict)
        if not fetched_record:
            match_date_id = star_schema_manager.insert_into_table("event_fact_table", fact_table_content, id=None)
        else:
            attribute_to_inc = star_schema_manager.get_dict_key_for_increment(fact_table_facts_dict, 1)
            # print(f"{i} Already in the fact table")
            star_schema_manager.increment_attribute_in_record("event_fact_table", fact_table_fk_dict, attribute_to_inc)
        # if i % 1000 == 0: print(f"{i} records done")


def fill_in_event_fact_table_assists(league_manager):
    records = league_manager.query(english_assists)

    print(f"Processing english assists - {len(records)} records.")
    for i, record in enumerate(records[:]):
        # print(record)

        venue_city = record[5].split('-')
        result = league_manager.get_result_from_goals(record[6], record[7])
        season = f"{record[8].year}-{record[9].year}"

        player_id, match_date_id, match_id, league_team_id = insert_into_common_tables(league_manager,
            name=record[0].strip(), position=record[3].strip(), birth_date=record[2], nationality=record[1].strip(), match_date=record[4], season=season,
            result=result, home_team=record[11], playing_team=record[10], venue=venue_city[0]
        )
        event_time_id = star_schema_manager.insert_into_table("event_time", {
            "time": str(record[13]),
            "half": str(league_manager.get_half_from_minute(record[13]))
        })

        fact_table_fk_dict = {"player_id": str(player_id), "match_date_id": str(match_date_id), "match_id": str(match_id), "league_team_id": str(league_team_id), "event_time_id": str(event_time_id), }
        fact_table_facts_dict = {"count_goals": str(0), "count_own_goals": str(0), "count_yellow_cards": str(0), "count_red_cards": str(0), "count_assists": str(1), "count_substitution_in": str(0), "count_substitution_off": str(0)}
        fact_table_content = dict(fact_table_fk_dict, **fact_table_facts_dict)
        # print(fact_table_facts_dict)

        fetched_record = star_schema_manager.fact_event_table_record_exists("event_fact_table", fact_table_fk_dict)
        if not fetched_record:
            match_date_id = star_schema_manager.insert_into_table("event_fact_table", fact_table_content, id=None)
        # else: # TABLE assists carries information also about goal scorers.. not only about players that assisted on the goals
        #     attribute_to_inc = star_schema_manager.get_dict_key_for_increment(fact_table_facts_dict, 1)
        #     print(f"{i} Already in the fact table")
        #     star_schema_manager.increment_attribute_in_record("event_fact_table", fact_table_fk_dict, attribute_to_inc)
        # if i % 1000 == 0: print(f"{i} records done")


def fill_in_event_fact_table_substitutions(league_manager):
    records = league_manager.query(english_substitutions)

    print(f"Processing english subs - {len(records)*2} records.")
    for i, record in enumerate(records[:]):
        # print(record)
        venue_city = record[5].split('-')
        result = league_manager.get_result_from_goals(record[6], record[7])
        season = f"{record[8].year}-{record[9].year}"

        #SUB ON
        player_id, match_date_id, match_id, league_team_id = insert_into_common_tables(league_manager,
            name=record[0].strip(), position=record[3].strip(), birth_date=record[2], nationality=record[1].strip(), match_date=record[4], season=season,
            result=result, home_team=record[11], playing_team=record[10], venue=venue_city[0]
        )
        event_time_id = star_schema_manager.insert_into_table("event_time", {
            "time": str(record[13]),
            "half": str(league_manager.get_half_from_minute(record[13]))
        })

        fact_table_fk_dict = { "player_id": str(player_id), "match_date_id": str(match_date_id), "match_id": str(match_id), "league_team_id": str(league_team_id), "event_time_id": str(event_time_id)}
        fact_table_facts_dict = { "count_goals": str(0), "count_own_goals": str(0), "count_yellow_cards": str(0), "count_red_cards": str(0), "count_assists": str(0), "count_substitution_in": str(1), "count_substitution_off": str(0)}
        fact_table_content = dict(fact_table_fk_dict, **fact_table_facts_dict)
        # print(fact_table_facts_dict)

        fetched_record = star_schema_manager.fact_event_table_record_exists("event_fact_table", fact_table_fk_dict)
        if not fetched_record:
            match_date_id = star_schema_manager.insert_into_table("event_fact_table", fact_table_content, id=None)
        else:
            attribute_to_inc = star_schema_manager.get_dict_key_for_increment(fact_table_facts_dict, 1)
            # print(f"{i} Already in the fact table")
            star_schema_manager.increment_attribute_in_record("event_fact_table", fact_table_fk_dict, attribute_to_inc)

        #SUB OFF
        player_id, match_date_id, match_id, league_team_id = insert_into_common_tables(league_manager,
            name=record[14].strip(), position=record[17].strip(), birth_date=record[16], nationality=record[15].strip(), match_date=record[4], season=season,
            result=result, home_team=record[11], playing_team=record[10], venue=venue_city[0]
        )
        event_time_id = star_schema_manager.insert_into_table("event_time", {
            "time": str(record[13]),
            "half": str(league_manager.get_half_from_minute(record[13]))
        })

        fact_table_fk_dict = { "player_id": str(player_id), "match_date_id": str(match_date_id), "match_id": str(match_id), "league_team_id": str(league_team_id), "event_time_id": str(event_time_id)}
        fact_table_facts_dict = { "count_goals": str(0), "count_own_goals": str(0), "count_yellow_cards": str(0), "count_red_cards": str(0), "count_assists": str(0), "count_substitution_in": str(0), "count_substitution_off": str(1)}
        # print(fact_table_facts_dict)
        fact_table_content = dict(fact_table_fk_dict, **fact_table_facts_dict)

        fetched_record = star_schema_manager.fact_event_table_record_exists("event_fact_table", fact_table_fk_dict)
        if not fetched_record:
            match_date_id = star_schema_manager.insert_into_table("event_fact_table", fact_table_content, id=None)
        else:
            attribute_to_inc = star_schema_manager.get_dict_key_for_increment(fact_table_facts_dict, 1)
            # print(f"{i*2} Already in the fact table")
            star_schema_manager.increment_attribute_in_record("event_fact_table", fact_table_fk_dict, attribute_to_inc)
        # if i % 1000 == 0: print(f"{i*2} records done")


if __name__ == '__main__':
    # fill_in_english_league()
    pass