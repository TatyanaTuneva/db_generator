import psycopg2
from configparser import ConfigParser


class DBManipulator:
    config = None
    connection = None
    cursor = None
    params = None

    def __init__(self, config_path):
        self.__read_config(config_path)
        pass

    def __read_config(self, path):
        parser = ConfigParser()
        section = 'postgresql'
        parser.read(path)
        params = {}
        if parser.has_section(section):
            for item in parser.items(section):
                params[item[0]] = item[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, path))
        self.params = params

    def connect(self):
        try:
            print('DEBUG: connecting to the PostgreSQL database...')
            self.connection = psycopg2.connect(**self.params)
            self.cursor = self.connection.cursor()

            print('DEBUG: testing connection...')
            print('PostgreSQL database version:')
            self.cursor.execute('SELECT version()')

            # display the PostgreSQL database server version
            db_version = self.cursor.fetchone()
            print(db_version)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        print('DEBUG: connecting to the PostgreSQL database is successful.')

    def disconnect(self):
        try:
            print('DEBUG: disconnecting to the PostgreSQL database...')
            self.cursor.close()
            self.connection.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        print('DEBUG: disconnecting to the PostgreSQL database is successful.')

    def get_tables(self):
        self.cursor.execute("""SELECT table_name FROM information_schema.tables
               WHERE table_schema = 'public'""")
        result = self.cursor.fetchall()
        return result

    def drop_all_users(self):
        self.cursor.execute("""DELETE FROM users""")

    def drop_all_categories(self):
        self.cursor.execute("""DELETE FROM categories""")

    def drop_all_messages(self):
        self.cursor.execute("""DELETE FROM messages""")
