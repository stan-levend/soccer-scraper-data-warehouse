from db_manager import DatabaseManager


def main():
    league_manager = DatabaseManager('postgres', 'postgres', 'tassu')
    # index = manager.get_index_if_exists('match', {'date': '2018-08-18', 'home_goals': 2, 'away_goals': 3, 'season': 2018})
    # index = manager.insert_into_table('building', {'city': 'kosice', 'name': 'test', 'street': 'sturova 10'})
    star_schema_manager = DatabaseManager('postgres', 'postgres', 'star_schema')

    records = league_manager.query('''
        SELECT player.name, player.position, player.number, player.birth_date, player.nationality, club_home.name as away_team, club_away.name as home_team, club.name, match.date, match.season, match.result, team_player.substitute, event.type, event.minute
        FROM event
            JOIN team_player
                ON event.team_player_id=team_player.id
            JOIN match
                ON event.match_id=match.id
            JOIN player
                ON team_player.player_id=player.id
            JOIN club as club_home
                ON match.home_team_id=club_home.id
            JOIN club as club_away
                ON match.away_team_id=club_away.id
            JOIN club
                ON team_player.club_id=club.id
    ''')

    for record in records[:5]:
        # name = record[:3]
        match_date = record[5]

        season = str(record[3])
        season += f"-{record[3]+1}"

        match_date_id = star_schema_manager.insert_into_table("match_date", {
            "season": season,
            "year": match_date.year

        })
        match_date_id = star_schema_manager.insert_into_table("match_date", {
            "season": season,
            "year": match_date.year

        })
        match_date_id = star_schema_manager.insert_into_table("match_date", {
            "season": season,
            "year": match_date.year

        })
        content = {
            "match_date_id": match_date_id,
            "match_date_id": match_date_id,
            "match_date_id": match_date_id,
            "match_date_id": match_date_id,
            "year": match_date.year,
            "goals_count": 1 if record[10] == "goal" else 0,
            "red_cards_count": 1 if record[10] == "yellow_card" else 0,
            "yellow_cards_count": 1 if record[10] == "goal" else 0,
        }

        result = star_schema_manager.get_index_if_exists("fact_table", content)
        if not result:
            match_date_id = star_schema_manager.insert_into_table("fact_table", content)
        else:
            star_schema_manager.increment_attribute_in_record("fact_table", result, "goals_count")



    league_manager.close_connection()