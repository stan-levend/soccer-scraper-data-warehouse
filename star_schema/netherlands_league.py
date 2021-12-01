import time
from db_manager import DatabaseManager

countries_df = pd.read_csv(f"matches.csv", usecols=['FTR', 'HomeTeam', 'AwayTeam'])
    # print(countries_df.name.unique())
    def get_country_by_code(code):
        try: return countries_df.loc[(countries_df['Date'] == datum) & (countries_df['HomeTeam'] == team1) & (countries_df['AwayTeam'] == team2), 'FTR'].iloc[0]
        except: raise ValueError(f'{code} country problem')


def main():
    league_manager = DatabaseManager('postgres', 'postgres', 'tassu-holandska_liga')
    # index = manager.get_index_if_exists('match', {'date': '2018-08-18', 'home_goals': 2, 'away_goals': 3, 'season': 2018})
    # index = manager.insert_into_table('building', {'city': 'kosice', 'name': 'test', 'street': 'sturova 10'})
    star_schema_manager = DatabaseManager('postgres', 'postgres', 'star_schema')

    records = league_manager.query('''
        SELECT player.name,  player.birth_date, player.nation, match.league, club_home.name as away_team, club_away.name as home_team, team.name, match.kickoff_date, match.season, match.venue, event.event_type, event.event_min, event.half
        FROM event
            JOIN match
                ON event.match_id=match.match_id
            JOIN team
                ON event.team_id=team.team_id
            JOIN player
                ON event.player_id=player.player_id
            JOIN team as club_home
                ON match.h_team_id=club_home.team_id
            JOIN team as club_away
                ON match.a_team_id=club_away.team_id
            WHERE match.season != '2018-2017' AND event.event_min IS NOT NULL AND event.half IS NOT NULL AND event.event_type = 'goal'  OR event.event_type = 'assist' OR event.event_type = 'yellow_card' OR event.event_type = 'red_card' OR event.event_type = 'substitution_in' OR event.event_type = 'substitution_out' OR event.event_type = 'own_goal'
''')

    print(f"Processing {len(records)}.")
    for i, record in enumerate(records[:]):
        # print(record)
        player_id = star_schema_manager.insert_into_table("player", {
            "name": record[0].replace("'", " "),
            "position": record[1],
            "birth_year": str(record[2].year),
            "birth_month": record[2].strftime("%B"),
            "nationality": record[3].replace("'", " ")
        })

        match_date = record[7]
        match_date_id = star_schema_manager.insert_into_table("match_date", {
            "season": str(record[8]),
            "year": str(match_date.year),
            "month": match_date.strftime("%B"),
            "day": str(match_date.day),
            "quarter": str(league_manager.get_quarter_from_date(match_date)),
            "day_of_week": league_manager.get_day_of_week_from_date(match_date),
            "weekend": str(league_manager.get_weekend_flag_from_date(match_date))
        })

        match_id = star_schema_manager.insert_into_table("match", {
            "venue": league_manager.get_italian_venue_from_team(record[4]),
            "result": league_manager.get_result_from_char(record[9]),
            "team": "Home" if record[4] == record[6] else "Away"
        })

        league_team_id = star_schema_manager.insert_into_table("league_team", {
            "league_name": record[4],
            "team_name": record[6]
        })

        minute = league_manager.get_minute_from_string(record[12]),

        event_time_id = star_schema_manager.insert_into_table("event_time", {
            "time": str(minute[0]),
            "half": str(record[13])
        })


        fact_table_fk_dict = {
            "player_id": str(player_id), "match_date_id": str(match_date_id), "match_id": str(match_id), "league_team_id": str(league_team_id), "event_time_id": str(event_time_id),
        }
        fact_table_facts_dict = {
            "count_goals": str(1 if record[11] == "goal" else 0),
            "count_own_goals": str(1 if record[11] == "own goal" else 0),
            "count_yellow_cards": str(1 if record[11] == "yellow card" else 0),
            "count_red_cards": str(1 if record[11] == "red card" else 0),
            "count_assists": str(1 if record[11] == "assist" else 0),
            "count_substitution_in": str(1 if record[11] == "sub on" else 0),
            "count_substitution_off": str(1 if record[11] == "sub off" else 0)
        }

        fact_table_content = dict(fact_table_fk_dict, **fact_table_facts_dict)

        if not star_schema_manager.fact_event_table_record_exists("event_fact_table", fact_table_fk_dict):
            match_date_id = star_schema_manager.insert_into_table("event_fact_table", fact_table_content, id=None)
            # print("Not in the fact table")
        else:
            attribute_to_inc = star_schema_manager.get_dict_key_for_increment(fact_table_facts_dict, 1)
            # print(attribute_to_inc)
            print(f"{i} Already in the fact table")
            time.sleep(1)
            star_schema_manager.increment_attribute_in_record("event_fact_table", fact_table_fk_dict, attribute_to_inc)
        if i % 1000 == 0: print(f"{i} records done")

    league_manager.close_connection()
    star_schema_manager.close_connection()


if __name__ == '__main__':
    main()
    pass
