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
                WHERE event.type = 'sub ON' or event.type = 'sub off'
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

spain_goals_assists='''
    SELECT player.name, player.pos, player.bornyear, player.nation, match.dateandtime, match.team1_score, team2_score, venue.stadion_name, season.seasonyear, goals.minute, goals.own,
    assist_player.name, assist_player.pos, assist_player.bornyear, assist_player.nation
        FROM goals
            JOIN match
                ON goals.matchid=match.id
            JOIN player
                ON goals.playerid=player.id
            left JOIN player as assist_player
                ON goals.assistid=assist_player.id
            JOIN venue
                ON match.venueid=venue.id
            JOIN season
                ON match.seasonid=season.id
            WHERE season.seasonyear != '2017-2018'
'''

spain_cards='''
SELECT player.name, player.pos, player.bornyear, player.nation, match.dateandtime, match.team1_score, team2_score, venue.stadion_name, season.seasonyear, cards.minute, cards.cardcolor
        FROM cards
            JOIN match
                ON cards.matchid=match.id
            JOIN player
                ON cards.playerid=player.id
            JOIN venue
                ON match.venueid=venue.id
            JOIN season
                ON match.seasonid=season.id
            WHERE season.seasonyear != '2017-2018'
'''
spain_substitution='''
    SELECT player_in.name, player_in.pos, player_in.bornyear, player_in.nation, match.dateandtime, match.team1_score, team2_score, venue.stadion_name, season.seasonyear,
    substitutions.minute, player_out.name, player_out.pos, player_out.bornyear, player_out.nation
        FROM substitutions
            JOIN match
                ON substitutions.matchid=match.id
            JOIN player as player_in
                ON substitutions.substitutingid=player_in.id
            JOIN player as player_out
                ON substitutions.substitutedid=player_out.id
            JOIN venue
                ON match.venueid=venue.id
            JOIN season
                ON match.seasonid=season.id
			WHERE season.seasonyear != '2017-2018'
'''

spain_lineups='''
    SELECT player.name, player.pos, player.bornyear, player.nation, match.dateandtime, match.team1_score, team2_score, venue.stadion_name, season.seasonyear,  team.teamname, playingbench.home, playingbench.playing
		FROM playingbench
			JOIN match
				ON playingbench.matchid=match.id
			JOIN player
				ON playingbench.playerid=player.id
			JOIN team
				ON playingbench.teamid=team.id
            JOIN season
                ON match.seasonid=season.id
			JOIN venue
                ON match.venueid=venue.id
			WHERE season.seasonyear != '2017-2018'

'''

german_events = '''
    SELECT DISTINCT
	CONCAT(player.firstname, ' ', player.lastname) as name,
	player_lineup.player_position,
	player.birthday,
	country.name as nationality,
	club_home.name as home_team,
	club_away.name as away_team,
	team.name as team,
	match.match_start,
	season.name as season,
	match.home_score,
	match.away_score,
	event_type.name as event_type,
	event.minute,
	event.own_goal,
	venue.name as venue,
    related_player.id

    FROM bundesliga.event
		JOIN bundesliga.player
			ON event.player_id=player.id
		Left JOIN bundesliga.player as related_player
			ON related_player.id = event.related_player_id
		JOIN bundesliga.lineup as player_lineup
			ON player.id=player_lineup.player_id
		JOIN bundesliga.country
			ON player.country_id=country.id
		JOIN bundesliga.team
			ON player_lineup.team_id=team.id
		JOIN bundesliga.match
			ON event.match_id = match.id
		JOIN bundesliga.season
			ON match.season_id = season.id
		JOIN bundesliga.team as club_home
			ON match.home_team_id = club_home.id
		JOIN bundesliga.team as club_away
			ON match.away_team_id = club_away.id
		JOIN bundesliga.event_type
			ON event.event_type_id = event_type.id
		JOIN bundesliga.venue
			ON match.venue_id = venue.id

	WHERE event_type.name != 'injury' AND event_type.name != 'back from injury'
'''

german_linups = '''
    SELECT
        CONCAT(player.firstname, ' ', player.lastname) as name,
        lineup.player_position,
        player.birthday,
        country.name as nationality,
        club_home.name as home_team,
        club_away.name as away_team,
        team.name as team,
        match.match_start,
        season.name as season,
        match.home_score,
        match.away_score,
        venue.name as venue

    FROM bundesliga.lineup
        JOIN bundesliga.player
            ON player.id = lineup.player_id
        JOIN bundesliga.match
            ON match.id = lineup.match_id
        JOIN bundesliga.team
            ON team.id = lineup.team_id
        JOIN bundesliga.country
            ON player.country_id = country.id
        JOIN bundesliga.team as club_home
            ON match.home_team_id = club_home.id
        JOIN bundesliga.team as club_away
            ON match.away_team_id = club_away.id
        JOIN bundesliga.venue
            ON match.venue_id = venue.id
        JOIN bundesliga.season
            ON match.season_id = season.id
'''

german_subs = '''
    SELECT DISTINCT
        CONCAT(player.firstname, ' ', player.lastname) as name,
        player_lineup.player_position,
        player.birthday,
        country.name as nationality,
        club_home.name as home_team,
        club_away.name as away_team,
        team.name as team,
        match.match_start,
        season.name as season,
        match.home_score,
        match.away_score,
        event_type.name as event_type,
        event.minute,
        venue.name as venue,
        related_player.id

    FROM bundesliga.event
		JOIN bundesliga.player
			ON event.player_id=player.id
		Left JOIN bundesliga.player as related_player
			ON related_player.id = event.related_player_id
		JOIN bundesliga.lineup as player_lineup
			ON player.id=player_lineup.player_id
		JOIN bundesliga.country
			ON player.country_id=country.id
		JOIN bundesliga.team
			ON player_lineup.team_id=team.id
		JOIN bundesliga.match
			ON event.match_id = match.id
		JOIN bundesliga.season
			ON match.season_id = season.id
		JOIN bundesliga.team as club_home
			ON match.home_team_id = club_home.id
		JOIN bundesliga.team as club_away
			ON match.away_team_id = club_away.id
		JOIN bundesliga.event_type
			ON event.event_type_id = event_type.id
		JOIN bundesliga.venue
			ON match.venue_id = venue.id

	WHERE event_type.name = 'substitution'
'''
dutch_event='''
SELECT player.name, lineup.pos,  player.birth_date, player.nation, match.kickoff_date, match.season, match.league, club_home.name as home_team, club_away.name as away_team, team.name, match.venue, event.event_type, event.event_min, event.half, lineup.minutes
        FROM event
            JOIN match
                ON event.match_id=match.match_id
            JOIN team
                ON event.team_id=team.team_id
            JOIN player
                ON event.player_id=player.player_id
            FULL JOIN lineup
                ON player.player_id=lineup.player_id AND match.match_id=lineup.match_id
            JOIN team as club_home
                ON match.h_team_id=club_home.team_id
            JOIN team as club_away
                ON match.a_team_id=club_away.team_id
            WHERE match.season != '2018-2017' AND event.event_min IS NOT NULL AND event.half IS NOT NULL AND event.event_type = 'goal'  OR event.event_type = 'assist' OR event.event_type = 'yellow_card' OR event.event_type = 'red_card' OR event.event_type = 'substitution_in' OR event.event_type = 'substitution_out' OR event.event_type = 'own_goal'
'''

dutch_lineup='''
SELECT player.name, lineup.pos, player.birth_date, player.nation, match.league, club_home.name as home_team, club_away.name as away_team, team.name, match.kickoff_date, match.season, match.venue, lineup.minutes
        FROM lineup
            JOIN match
                ON lineup.match_id=match.match_id
            JOIN team
                ON lineup.team_id=team.team_id
            JOIN player
                ON lineup.player_id=player.player_id
            JOIN team as club_home
                ON match.h_team_id=club_home.team_id
            JOIN team as club_away
                ON match.a_team_id=club_away.team_id
            WHERE match.season != '2018-2017'

'''
