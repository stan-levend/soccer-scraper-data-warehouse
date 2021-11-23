import psycopg2
from psycopg2 import Error


class DatabaseManager():

    def __init__(self, user, password, database) -> None:
        self.connection = None
        try:
            self.connection = psycopg2.connect(user=f"{user}",
                                  password=f"{password}",
                                  host="127.0.0.1",
                                  port="5432",
                                  database=f"{database}")
            self.cursor = self.connection.cursor()
        except (Exception, Error) as error:
            print("Error while connecting to PostgreSQL", error)
            self.close_connection()


    def close_connection(self):
        if (self.connection):
            self.cursor.close()
            self.connection.close()
            print("PostgreSQL connection is closed.")


    def get_index_if_exists(self, table, content) -> int: #TODO: Add support for automatic attributes?
        try:
            condition_string = ''
            for index, (attribute, value) in enumerate(content.items()):
                condition_string += f"{table}.{attribute}='{value}'"
                if index+1 != len(content): condition_string += " AND"

            sql_query = f"SELECT {table}.id FROM {table} WHERE {condition_string}"

            self.cursor.execute(sql_query)
            record = self.cursor.fetchone()
        except (Exception, Error) as error:
            print(f"Error while fetching index from {table}", error)
            self.close_connection()
        return record[0] if record is not None else None


    def insert_into_table(self, table, content):
        try:
            attributes = ', '.join(content.keys())
            values = "', '".join(content.values())
            # sql_query = f"INSERT INTO {table}({attributes}) VALUES('{values}') RETURNING id;"
            condition_string = ''
            for index, (attribute, value) in enumerate(content.items()):
                condition_string += f" {attribute}='{value}'"
                if index+1 != len(content): condition_string += " AND"
            sql_query = f'''
                WITH s AS (
                    SELECT id, {attributes}
                    FROM {table}
                    WHERE {condition_string}
                ), i AS (
                    INSERT INTO {table} ({attributes})
                    SELECT '{values}'
                    WHERE NOT EXISTS (SELECT 1 FROM s)
                    RETURNING id, {attributes}
                )
                SELECT id, {attributes}
                FROM i
                UNION ALL
                SELECT id, {attributes}
                FROM s
            '''

            self.cursor.execute(sql_query)
            id = self.cursor.fetchone()[0]
            self.connection.commit()
            return id
        except (Exception, Error) as error:
            print(f"Error while inserting into {table}", error)
            self.close_connection()


    def update_record(self, table, id, content):
        try:
            update_string = ''
            for index, (attribute, value) in enumerate(content.items()):
                update_string += f"{attribute}='{value}'"
                if index+1 != len(content): update_string += ','

            sql_query = f"UPDATE {table} SET {update_string} WHERE id={id} RETURNING id"

            self.cursor.execute(sql_query)
            id = self.cursor.fetchone()[0]
            self.connection.commit()
            return id
        except (Exception, Error) as error:
            print(f"Error while updating record {id} in {table}", error)
            self.close_connection()


    def query(self, query_string):
        self.cursor.execute(query_string)
        return self.cursor.fetchall()


    def increment_attribute_in_record(self, table, id, attribute):
        try:
            sql_query = f"UPDATE {table} SET {attribute} = {attribute} + 1 WHERE id={id}"

            self.cursor.execute(sql_query)
            self.connection.commit()
        except (Exception, Error) as error:
            print(f"Error while updating record {id} in {table}", error)
            self.close_connection()


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