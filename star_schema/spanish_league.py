import pandas as pd
import time
from query_strings import spain_goals_assists, spain_cards, spain_substitution, spain_lineups
from db_manager import DatabaseManager


star_schema_manager = DatabaseManager('postgres', 'postgres', 'star_schema')
countries_df = pd.read_csv(f"star_schema/world.csv", usecols=['name', 'alpha3'])


def get_country_by_code(code):
    try: return countries_df.loc[countries_df['alpha3'] == str(code).lower(), 'name'].iloc[0]
    except: raise ValueError(f'{code} country problem')


def fill_in_spanish_league(league_manager):

    fill_in_event_fact_table_cards(league_manager)
    fill_in_event_fact_table_goals_assists(league_manager)
    fill_in_event_fact_table_substitutions(league_manager)
    # fill_in_lineup_fact_table(league_manager)

    league_manager.close_connection()
    star_schema_manager.close_connection()


def insert_into_common_tables(league_manager, name, position, birth_date, nationality, match_date, season, result, home_team, playing_team, venue):
    player_id = star_schema_manager.insert_into_table("player", {
        "name": name.replace("'", " "),
        "position": position,
        "birth_year": str(birth_date),
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
        "team": "Home" if home_team is True else "Away"
    })

    league_team_id = star_schema_manager.insert_into_table("league_team", {
        "league_name": "La liga",
        "team_name": playing_team
    })

    return player_id, match_date_id, match_id, league_team_id


def fill_in_event_fact_table_cards(league_manager):
    records = league_manager.query(spain_cards)

    print(f"Processing spanish cards - {len(records)} records.")
    for i, record in enumerate(records[:]):
        # print(record)

        country = get_country_by_code(record[3])
        result = league_manager.get_result_from_goals(record[5], record[6])
        team = league_manager.get_team_by_matchdate_and_name(record[4], record[0])
        if team is None: continue
        home_team_flag = True if team[0] == 'TRUE' else False

        player_id, match_date_id, match_id, league_team_id = insert_into_common_tables(league_manager,
            name=record[0].strip(), position=record[1].strip(), birth_date=record[2], nationality=country, match_date=record[4], season=record[8],
            result=result, home_team=home_team_flag, playing_team=team[1], venue=record[7]
        )

        event_time_id = star_schema_manager.insert_into_table("event_time", {
            "time": str(record[9]),
            "half": str(league_manager.get_half_from_minute(record[9]))
        })

        # print(player_id, match_date_id, match_id, league_team_id, event_time_id)

        fact_table_fk_dict = {"player_id": str(player_id), "match_date_id": str(match_date_id), "match_id": str(match_id), "league_team_id": str(league_team_id), "event_time_id": str(event_time_id), }
        fact_table_facts_dict = {"count_goals": str(0), "count_own_goals": str(0), "count_assists": str(0), "count_substitution_in": str(0), "count_substitution_off": str(0),
                                 "count_yellow_cards": str(1) if record[10] == 'Yellow' else str(0),
                                 "count_red_cards": str(1) if record[10] == 'Red' else str(0),
                                }
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


def fill_in_event_fact_table_goals_assists(league_manager):
    records = league_manager.query(spain_goals_assists)

    print(f"Processing spanish goals and assists - {len(records*2)} records.")
    for i, record in enumerate(records[:]):
        # print(record)
        #GOALS
        # if record[11] is None:
        #     print("none")
        # continue
        country = get_country_by_code(record[3])
        team = league_manager.get_team_by_matchdate_and_name(record[4], record[0])
        if team is None:
            team = league_manager.get_team_by_matchdate_and_name(record[4], record[11])
        if team is None: continue

        result = league_manager.get_result_from_goals(record[5], record[6])
        # print(result, country, team)
        home_team_flag = True if team[0] == 'TRUE' else False

        player_id, match_date_id, match_id, league_team_id = insert_into_common_tables(league_manager,
            name=record[0].strip(), position=record[1].strip(), birth_date=record[2], nationality=country, match_date=record[4], season=record[8],
            result=result, home_team=home_team_flag, playing_team=team[1], venue=record[7]
        )

        event_time_id = star_schema_manager.insert_into_table("event_time", {
            "time": str(record[9]),
            "half": str(league_manager.get_half_from_minute(record[9]))
        })

        # print(player_id, match_date_id, match_id, league_team_id, event_time_id)

        fact_table_fk_dict = {"player_id": str(player_id), "match_date_id": str(match_date_id), "match_id": str(match_id), "league_team_id": str(league_team_id), "event_time_id": str(event_time_id), }
        fact_table_facts_dict = {"count_goals": str(1) if record[10] == 'false' else str(0),
                                 "count_own_goals": str(1) if record[10] == 'true' else str(0),
                                 "count_yellow_cards": str(0), "count_red_cards": str(0), "count_assists": str(0), "count_substitution_in": str(0), "count_substitution_off": str(0)}
        fact_table_content = dict(fact_table_fk_dict, **fact_table_facts_dict)
        # print(fact_table_facts_dict)

        fetched_record = star_schema_manager.fact_event_table_record_exists("event_fact_table", fact_table_fk_dict)
        if not fetched_record:
            match_date_id = star_schema_manager.insert_into_table("event_fact_table", fact_table_content, id=None)
        else:
            attribute_to_inc = star_schema_manager.get_dict_key_for_increment(fact_table_facts_dict, 1)
            # print(f"{i} Already in the fact table")
            star_schema_manager.increment_attribute_in_record("event_fact_table", fact_table_fk_dict, attribute_to_inc)

        # ASSISTS
        if record[11] is None:
            continue
        country = get_country_by_code(record[14])
        player_id, match_date_id, match_id, league_team_id = insert_into_common_tables(league_manager,
            name=record[11].strip(), position=record[12].strip(), birth_date=record[13], nationality=country, match_date=record[4], season=record[8],
            result=result, home_team=home_team_flag, playing_team=team[1], venue=record[7]
        )

        event_time_id = star_schema_manager.insert_into_table("event_time", {
            "time": str(record[9]),
            "half": str(league_manager.get_half_from_minute(record[9]))
        })

        fact_table_fk_dict = { "player_id": str(player_id), "match_date_id": str(match_date_id), "match_id": str(match_id), "league_team_id": str(league_team_id), "event_time_id": str(event_time_id)}
        fact_table_facts_dict = { "count_goals": str(0), "count_own_goals": str(0), "count_yellow_cards": str(0), "count_red_cards": str(0), "count_assists": str(1), "count_substitution_in": str(0), "count_substitution_off": str(0)}
        # print(fact_table_facts_dict)
        fact_table_content = dict(fact_table_fk_dict, **fact_table_facts_dict)

        fetched_record = star_schema_manager.fact_event_table_record_exists("event_fact_table", fact_table_fk_dict)
        if not fetched_record:
            match_date_id = star_schema_manager.insert_into_table("event_fact_table", fact_table_content, id=None)
        else:
            attribute_to_inc = star_schema_manager.get_dict_key_for_increment(fact_table_facts_dict, 1)
            # print(f"{i} Already in the fact table")
            star_schema_manager.increment_attribute_in_record("event_fact_table", fact_table_fk_dict, attribute_to_inc)

        # if i % 1000 == 0: print(f"{i*2} records done")


def fill_in_event_fact_table_substitutions(league_manager):
    records = league_manager.query(spain_substitution)

    print(f"Processing spanish subs - {len(records*2)} records.")
    for i, record in enumerate(records[:]):
        # print(record)

        country = get_country_by_code(record[3])
        team = league_manager.get_team_by_matchdate_and_name(record[4], record[0])
        if team is None:
            team = league_manager.get_team_by_matchdate_and_name(record[4], record[10])
        if team is None: continue

        result = league_manager.get_result_from_goals(record[5], record[6])
        # print(result, country, team)
        home_team_flag = True if team[0] == 'TRUE' else False

        player_id, match_date_id, match_id, league_team_id = insert_into_common_tables(league_manager,
            name=record[0].strip(), position=record[1].strip(), birth_date=record[2], nationality=country, match_date=record[4], season=record[8],
            result=result, home_team=home_team_flag, playing_team=team[1], venue=record[7]
        )

        event_time_id = star_schema_manager.insert_into_table("event_time", {
            "time": str(record[9]),
            "half": str(league_manager.get_half_from_minute(record[9]))
        })

        # print(player_id, match_date_id, match_id, league_team_id, event_time_id)

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

        # #SUB OFF
        country = get_country_by_code(record[13])
        player_id, match_date_id, match_id, league_team_id = insert_into_common_tables(league_manager,
            name=record[10].strip(), position=record[11].strip(), birth_date=record[12], nationality=country, match_date=record[4], season=record[8],
            result=result, home_team=home_team_flag, playing_team=team[1], venue=record[7]
        )

        event_time_id = star_schema_manager.insert_into_table("event_time", {
            "time": str(record[9]),
            "half": str(league_manager.get_half_from_minute(record[9]))
        })

        # print(player_id, match_date_id, match_id, league_team_id, event_time_id)

        fact_table_fk_dict = { "player_id": str(player_id), "match_date_id": str(match_date_id), "match_id": str(match_id), "league_team_id": str(league_team_id), "event_time_id": str(event_time_id)}
        fact_table_facts_dict = { "count_goals": str(0), "count_own_goals": str(0), "count_yellow_cards": str(0), "count_red_cards": str(0), "count_assists": str(0), "count_substitution_in": str(0), "count_substitution_off": str(1)}
        # print(fact_table_facts_dict)
        fact_table_content = dict(fact_table_fk_dict, **fact_table_facts_dict)

        fetched_record = star_schema_manager.fact_event_table_record_exists("event_fact_table", fact_table_fk_dict)
        if not fetched_record:
            match_date_id = star_schema_manager.insert_into_table("event_fact_table", fact_table_content, id=None)
        else:
            attribute_to_inc = star_schema_manager.get_dict_key_for_increment(fact_table_facts_dict, 1)
            # print(f"{i} Already in the fact table")
            star_schema_manager.increment_attribute_in_record("event_fact_table", fact_table_fk_dict, attribute_to_inc)

        # if i % 1000 == 0: print(f"{i*2} records done")

def fill_in_lineup_fact_table(league_manager):
    lineup_records = league_manager.query(spain_lineups)

    print(f"Processing spanish lineups - {len(lineup_records)} records.")
    for i, record in enumerate(lineup_records[:]):

        try: country = get_country_by_code(record[3])
        except: continue

        result = league_manager.get_result_from_goals(record[5], record[6])
        home_team_flag = True if record[10] == 'TRUE' else False

        player_id, match_date_id, match_id, league_team_id = insert_into_common_tables(league_manager,
            name=record[0].strip(), position=record[1].strip(), birth_date=record[2], nationality=country, match_date=record[4], season=record[8],
            result=result, home_team=home_team_flag, playing_team=record[9], venue=record[7]
        )
        is_playing= record[11]

        fact_table_fk_dict = {"player_id": str(player_id), "match_date_id": str(match_date_id), "match_id": str(match_id), "league_team_id": str(league_team_id)}
        fact_table_facts_dict = {"time_played": str(0) if is_playing == 'FALSE' else str(90)}
        # print(is_playing, fact_table_facts_dict)
        fact_table_content = dict(fact_table_fk_dict, **fact_table_facts_dict)

        # print(player_id, match_date_id, match_id, league_team_id)

        fetched_record = star_schema_manager.fact_event_table_record_exists("lineup_fact_table", fact_table_fk_dict)
        if not fetched_record:
            match_date_id = star_schema_manager.insert_into_table("lineup_fact_table", fact_table_content, id=None)
        else:
            # print(f"{i} Duplicate player, not inserting.")
            pass

        # if i % 1000 == 0: print(f"{i} records done")


    # WE NEED TO FIND SUBSTITUTIONS AND SUBSTRACT/ADD TIME TO THE TIME PLAYED THAT WE HAVE OBTAINED ALREADY
    substitution_records = league_manager.query(spain_substitution)
    print(f"Processing spanish substitution_records for lineups - {len(substitution_records)} records.")
    for i, record in enumerate(substitution_records[:]):
        # print(record)
        country = get_country_by_code(record[13])
        team = league_manager.get_team_by_matchdate_and_name(record[4], record[0])
        if team is None:
            team = league_manager.get_team_by_matchdate_and_name(record[4], record[10])
        if team is None: continue

        result = league_manager.get_result_from_goals(record[5], record[6])
        # print(result, country, team)
        home_team_flag = True if [0] == 'TRUE' else False

        player_id, match_date_id, match_id, league_team_id = insert_into_common_tables(league_manager,
            name=record[0].strip(), position=record[1].strip(), birth_date=record[2], nationality=country, match_date=record[4], season=record[8],
            result=result, home_team=home_team_flag, playing_team=team[1], venue=record[7]
        )
        fact_table_fk_dict = {"player_id": str(player_id), "match_date_id": str(match_date_id), "match_id": str(match_id), "league_team_id": str(league_team_id)}
        # print(player_id, match_date_id, match_id, league_team_id)

        event_time = record[9]
        fetched_record = star_schema_manager.fact_event_table_record_exists("lineup_fact_table", fact_table_fk_dict)
        if fetched_record is not None:
            if event_time > 90: new_time = event_time - 90
            else: new_time = 90 - event_time
            star_schema_manager.update_record("lineup_fact_table", fact_table_fk_dict, "time_played", new_time)
        else:
            # print("Sub in record not found")
            pass

        #SUB OFF
        try: country = get_country_by_code(record[13])
        except: continue
        player_id, match_date_id, match_id, league_team_id = insert_into_common_tables(league_manager,
            name=record[10].strip(), position=record[11].strip(), birth_date=record[12], nationality=country, match_date=record[4], season=record[8],
            result=result, home_team=home_team_flag, playing_team=team[1], venue=record[7]
        )
        fact_table_fk_dict = {"player_id": str(player_id), "match_date_id": str(match_date_id), "match_id": str(match_id), "league_team_id": str(league_team_id)}

        event_time = record[9]
        fetched_record = star_schema_manager.fact_event_table_record_exists("lineup_fact_table", fact_table_fk_dict)
        if fetched_record is not None:
            if event_time > 90: new_time = 90
            else: new_time = event_time
            star_schema_manager.update_record("lineup_fact_table", fact_table_fk_dict, "time_played", new_time)
        else:
            # print("Sub off record not found")
            pass

        # if i % 1000 == 0: print(f"{i} records done")

if __name__ == '__main__':
    # fill_in_spanish_league()
    pass