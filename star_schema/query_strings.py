italian_events='''
    SELECT player.name, player.position, player.birth_date, player.nationality, club_home.name as away_team, club_away.name as home_team, club.name, match.date, match.season, match.result, team_player.substitute, event.type, event.minute
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
                WHERE event.type != 'corner' AND event.type != 'offside'
    '''

italian_lineups='''
    SELECT player.name, player.position, player.birth_date, player.nationality, club_home.name as home_team, club_away.name as away_team, club.name, match.date, match.season, match.result, team_player.substitute
        FROM team_player
            JOIN match
                ON team_player.match_id=match.id
            JOIN player
                ON team_player.player_id=player.id
            JOIN club as club_home
                ON match.home_team_id=club_home.id
            JOIN club as club_away
                ON match.away_team_id=club_away.id
            JOIN club
                ON team_player.club_id=club.id
    '''

italian_subs='''
    SELECT player.name, player.position, player.birth_date, player.nationality, club_home.name as away_team, club_away.name as home_team, club.name, match.date, match.season, match.result, team_player.substitute, event.type, event.minute
    FROM team_player
        JOIN match
            ON team_player.match_id=match.id
        JOIN player
            ON team_player.player_id=player.id
        JOIN club as club_home
            ON match.home_team_id=club_home.id
        JOIN club as club_away
            ON match.away_team_id=club_away.id
        JOIN club
            ON team_player.club_id=club.id
        JOIN event
            ON event.team_player_id=team_player.id
                WHERE event.type = 'sub on' or event.type = 'sub off'
    '''

english_cards = '''
    SELECT 	players.name, players.nationality, players.date_of_birth, players.position, matches.date, matches.location, matches.home_score, matches.away_score,
        seasons.date_start, seasons.date_end, clubs.name as playing_team, home_club.name as home_club, away_club.name as away_team, cards.type, cards.time
    FROM cards
        JOIN matches
            ON cards.id_match=matches.id
        JOIN players
            ON cards.id_player=players.id
        JOIN seasons
            ON matches.id_season=seasons.id
        JOIN player_club
            ON player_club.id_player=players.id
        JOIN clubs
            ON player_club.id_club=clubs.id AND player_club.id_season=seasons.id
        JOIN clubs as home_club
            ON home_club.id=matches.id_club_home
        JOIN clubs as away_club
            ON away_club.id=matches.id_club_away
    '''

english_goals = '''
    SELECT 	players.name, players.nationality, players.date_of_birth, players.position, matches.date, matches.location, matches.home_score, matches.away_score,
            seasons.date_start, seasons.date_end, clubs.name as playing_team, home_club.name as home_club, away_club.name as away_team, goals.time
    FROM goals
        JOIN matches
            ON goals.id_match=matches.id
        JOIN players
            ON goals.id_player=players.id
        JOIN seasons
            ON matches.id_season=seasons.id
        JOIN player_club
            ON player_club.id_player=players.id
        JOIN clubs
            ON player_club.id_club=clubs.id AND player_club.id_season=seasons.id
        JOIN clubs as home_club
            ON home_club.id=matches.id_club_home
        JOIN clubs as away_club
            ON away_club.id=matches.id_club_away
    '''

english_assists='''
    SELECT 	players.name, players.nationality, players.date_of_birth, players.position, matches.date, matches.location, matches.home_score, matches.away_score,
		        seasons.date_start, seasons.date_end, clubs.name as playing_team, home_club.name as home_club, away_club.name as away_team, goals.time
    FROM assists
        JOIN goals
            ON assists.id_goal=goals.id
        JOIN matches
            ON goals.id_match=matches.id
        JOIN players
            ON assists.id_player=players.id
        JOIN seasons
            ON matches.id_season=seasons.id
        JOIN player_club
            ON player_club.id_player=players.id
        JOIN clubs
            ON player_club.id_club=clubs.id AND player_club.id_season=seasons.id
        JOIN clubs as home_club
            ON home_club.id=matches.id_club_home
        JOIN clubs as away_club
            ON away_club.id=matches.id_club_away
    '''

english_substitutions = '''
    SELECT 	player_in.name, player_in.nationality, player_in.date_of_birth, player_in.position,
            matches.date, matches.location, matches.home_score, matches.away_score,
            seasons.date_start, seasons.date_end, clubs.name as playing_team, home_club.name as home_club, away_club.name as away_team, substitutions.time,
            player_out.name, player_out.nationality, player_out.date_of_birth, player_out.position
    FROM substitutions
        JOIN matches
            ON substitutions.id_match=matches.id
        JOIN players as player_in
            ON substitutions.player_in=player_in.id
        JOIN players as player_out
            ON substitutions.player_out=player_out.id
        JOIN seasons
            ON matches.id_season=seasons.id
        JOIN player_club
            ON player_club.id_player=player_in.id
        JOIN clubs
            ON player_club.id_club=clubs.id AND player_club.id_season=seasons.id
        JOIN clubs as home_club
            ON home_club.id=matches.id_club_home
        JOIN clubs as away_club
            ON away_club.id=matches.id_club_away
    '''

