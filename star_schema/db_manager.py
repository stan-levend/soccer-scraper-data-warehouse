from datetime import datetime

import psycopg2
from psycopg2 import Error

from italian_venues import ITALIAN_VENUES


class DatabaseManager():

    def __init__(self, user: str, password: str, database: str) -> None:
        self.connection = None
        try:
            self.connection = psycopg2.connect(user=f"{user}",
                                  password=f"{password}",
                                  host="127.0.0.1",
                                  port="5432",
                                  database=f"{database}")
            self.cursor = self.connection.cursor()
        except (Exception, Error) as error:
            self.close_connection()
            raise Error("Error while connecting to PostgreSQL", error)


    def close_connection(self):
        if (self.connection):
            self.cursor.close()
            self.connection.close()
            print("PostgreSQL connection is closed.")


    def fact_event_table_record_exists(self, table: str, content: dict):
        try:
            condition_string = ''
            for index, (attribute, value) in enumerate(content.items()):
                condition_string += f"{table}.{attribute}='{value}'"
                if index+1 != len(content): condition_string += " AND "

            sql_query = f"SELECT * FROM {table} WHERE {condition_string}"
            # print(sql_query)
            self.cursor.execute(sql_query)
            record = self.cursor.fetchone()
            return record if record is not None else None
        except (Exception, Error) as error:
            self.close_connection()
            raise Error(f"Error while fetching index from {table}", error)


    def insert_into_table(self, table: str, content: dict, id: str = 'id') -> int:
        try:
            attributes = ', '.join(content.keys())
            values = "', '".join(content.values())
            # sql_query = f"INSERT INTO {table}({attributes}) VALUES('{values}') RETURNING id;"
            condition_string = ''
            for index, (attribute, value) in enumerate(content.items()):
                condition_string += f" {attribute}='{value}'"
                if index+1 != len(content): condition_string += " AND"
            id_string = f'{id},' if id is not None else ''
            sql_query = f'''
                WITH s AS (
                    SELECT {id_string} {attributes}
                    FROM {table}
                    WHERE {condition_string}
                ), i AS (
                    INSERT INTO {table} ({attributes})
                    SELECT '{values}'
                    WHERE NOT EXISTS (SELECT 1 FROM s)
                    RETURNING {id_string} {attributes}
                )
                SELECT {id_string} {attributes}
                FROM i
                UNION ALL
                SELECT {id_string} {attributes}
                FROM s
            '''

            self.cursor.execute(sql_query)
            id = self.cursor.fetchone()[0]
            self.connection.commit()
            return id
        except (Exception, Error) as error:
            self.close_connection()
            raise Error(f"Error while inserting into {table}", error)


    def update_record(self, table: str, content: dict, attribute_to_update: str, value_to_update: int) -> int:
        try:
            condition_string = ''
            for index, (attribute, value) in enumerate(content.items()):
                condition_string += f"{table}.{attribute}='{value}'"
                if index+1 != len(content): condition_string += " AND "

            sql_query = f"UPDATE {table} SET {attribute_to_update} = '{value_to_update}' WHERE {condition_string}"
            # print(sql_query)
            self.cursor.execute(sql_query)
            self.connection.commit()
        except (Exception, Error) as error:
            self.close_connection()
            raise Error(f"Error while updating record in {table}", error)

    def get_team_by_matchdate_and_name(self, matchdate, player_name):
        try:
            query_string = f'''
                SELECT playingbench.home, team.teamname
                    FROM playingbench
                        JOIN match
                            ON playingbench.matchid=match.id
                        JOIN player
                            ON playingbench.playerid=player.id
                        JOIN team
                            ON playingbench.teamid=team.id
                        WHERE match.dateandtime = '{str(matchdate)}' and player.name = '{str(player_name)}'
            '''
            self.cursor.execute(query_string)
            record = self.cursor.fetchone()
            return record if record is not None else None
        except (Exception, Error) as error:
            self.close_connection()
            raise Error(f"Error while fetching team_name and home/away team flag.", error)


    def get_player_info_by_player_id(self, player_id):
        try:
            query_string = f'''
                    SELECT
                        CONCAT(player.firstname, ' ', player.lastname) as name,
                        lineup.player_position,
                        player.birthday,
                        country.name as nationality

                    FROM bundesliga.lineup
                        JOIN bundesliga.player
                            ON player.id = lineup.player_id
                        JOIN bundesliga.match
                            ON match.id = lineup.match_id
                        JOIN bundesliga.team
                            ON team.id = lineup.team_id
                        JOIN bundesliga.country
                            ON team.country_id = country.id
                    WHERE player.id='{str(player_id)}'
            '''
            self.cursor.execute(query_string)
            record = self.cursor.fetchone()
            return record if record is not None else None
        except (Exception, Error) as error:
            self.close_connection()
            raise Error(f"Error while fetching team_name and home/away team flag.", error)


    def query(self, query_string: str) -> tuple:
        self.cursor.execute(query_string)
        return self.cursor.fetchall()


    def increment_attribute_in_record(self, table: str, content: dict, attribute_to_inc: str) -> None:
        try:
            condition_string = ''
            for index, (attribute, value) in enumerate(content.items()):
                condition_string += f"{table}.{attribute}='{value}'"
                if index+1 != len(content): condition_string += " AND "

            sql_query = f"UPDATE {table} SET {attribute_to_inc} = {attribute_to_inc} + 1 WHERE {condition_string}"

            self.cursor.execute(sql_query)
            self.connection.commit()
        except (Exception, Error) as error:
            self.close_connection()
            raise Error(f"Error while updating record in {table}", error)


    def get_quarter_from_date(self, date: datetime) -> int:
        return (date.month-1)//3 + 1

    def get_weekend_flag_from_date(self, date: datetime) -> bool:
        return True if date.weekday() < 5 else False

    def get_day_of_week_from_date(self, date: datetime) -> str:
        return date.strftime('%A')

    def get_result_from_char(self, char: str) -> str:
        if char=="H": return "Home"
        elif char=="D": return "Draw"
        elif char=="A": return "Away"
        else: raise ValueError("Wrong input, you can input only values 'H', 'D' or 'A'")

    def get_result_from_goals(self, home_goals: int, away_goals: int) -> str:
        if home_goals > away_goals: return "Home"
        elif home_goals < away_goals: return "Away"
        elif home_goals == away_goals: return "Draw"

    def get_italian_venue_from_team(self, team: str) -> str:
        venue = ITALIAN_VENUES.get(team, None)
        if not venue: raise ValueError("You have inserted wrong team name")
        return venue

    def get_minute_from_string(self, minute: str) -> int:
        split = minute.split('+')
        if len(split)==1:
            return int(split[0])
        else:
            return int(split[0]) + int(split[1])

    def get_half_from_minute(self, minute: int) -> int:
        return 1 if minute <= 47 else 2

    def get_dict_key_for_increment(self, dict: dict, value: int) -> str:
        return list(dict.keys())[list(dict.values()).index(str(value))]

def test():
    try:
        # Connect to an existing database
        connection = psycopg2.connect(user="postgres",
                                    password="postgres",
                                    host="127.0.0.1",
                                    port="5432",
                                    database="tassu")

        # Create a cursor to perform database operations
        cursor = connection.cursor()
        # Print PostgreSQL details
        print("PostgreSQL server information")
        print(connection.get_dsn_parameters(), "\n")
        # Executing a SQL query
        club_home='test'
        cursor.execute(f'''
            SELECT match.date, match.home_goals, match.away_goals, match.season
            FROM match
            WHERE match.date='2018-08-18' AND match.home_goals='2' AND match.away_goals='3' AND match.season='2018'
        ''')
        # Fetch result
        # field_names = [i[0] for i in cursor.description]
        # print(field_names)
        records = cursor.fetchone()
        manager = DatabaseManager()
        record = manager.get_index_if_exists('match', ['date', 'home_goals', 'away_goals', 'season'], ['2018-08-18', '2', '3', '2018'])
        print(record)
        # for record in records:
        #     print(record)

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

if __name__ == '__main__':
    # test()
    pass