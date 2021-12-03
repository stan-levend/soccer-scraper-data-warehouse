from db_manager import DatabaseManager
from spanish_league import fill_in_spanish_league
from german_league import fill_in_german_league
from italian_league import fill_in_italian_league
from english_league import fill_in_english_league


def fill_in_all_leagues():
    import os
    print(os.path.abspath(os.getcwd()))
    german_league_manager = DatabaseManager('postgres', 'postgres', 'german')
    fill_in_german_league(german_league_manager)

    # spanish_league_manager = DatabaseManager('postgres', 'postgres', 'spanish')
    # fill_in_spanish_league(spanish_league_manager)

    # italian_league_manager = DatabaseManager('postgres', 'postgres', 'tassu')
    # fill_in_italian_league(italian_league_manager)

    # english_league_manager = DatabaseManager('postgres', 'postgres', 'english')
    # fill_in_english_league(english_league_manager)


if __name__ == '__main__':
    fill_in_all_leagues()