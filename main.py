import psycopg2

from db_connector import DBManipulator
import random
import uuid

from psycopg2.extras import execute_batch

from mimesis.enums import Gender
from mimesis import Generic
from mimesis import Datetime

g = Generic('en')
d = Datetime('en')

users_count = 500000//20
categories_count = 5000//20
messages_count = 10000000//20


# users_count = 5
# categories_count = 10
# messages_count = 10


def create_users(count):
    result = [{'id': create_uuid(), 'name': g.person.full_name(gender=Gender.FEMALE)} for i in range(count)]
    return result


def create_categories(count):
    result = [{'id': create_uuid(), 'name': g.text.title(), 'parent_id': create_uuid()} for i in range(count)]
    return result


def create_messages(count, users, categories):
    result = [{'id': create_uuid(),
               'text': g.text.text(quantity=1),
               'category_id': categories[random.randint(0, len(categories) - 1)].get('id'),
               'posted_at': d.datetime(2000, 2020),
               'author_id': users[random.randint(0, len(users)) - 1].get('id')}
              for i in range(count)]
    return result


def create_uuid():
    return str(uuid.uuid1())


if __name__ == '__main__':
    db_con = DBManipulator('db.ini')
    db_con.connect()

    clean_start = True

    if clean_start:
        db_con.drop_all_users()
        db_con.drop_all_categories()
        db_con.drop_all_messages()

    users = create_users(users_count)
    categories = create_categories(categories_count)
    messages = create_messages(messages_count, users, categories)

    # print(db_con.get_tables())
    # db_con.cursor.execute("SELECT * from public.users")
    # print(db_con.cursor.fetchall())

    print('DEBUG: inserting users...')
    db_con.cursor.execute("PREPARE insert_users (uuid, text) AS INSERT INTO users VALUES($1, $2);")
    execute_batch(db_con.cursor, "EXECUTE insert_users (%(id)s, %(name)s)", users)
    db_con.connection.commit()
    print('DEBUG: inserting users successful.')

    print('DEBUG: inserting categories...')
    db_con.cursor.execute("PREPARE insert_categories (uuid, text, uuid) AS INSERT INTO categories VALUES($1, $2, $3);")
    execute_batch(db_con.cursor, "EXECUTE insert_categories (%(id)s, %(name)s, %(parent_id)s)", categories)
    db_con.connection.commit()
    print('DEBUG: inserting categories successful.')

    print('DEBUG: inserting messages...')
    db_con.cursor.execute(
        "PREPARE mes (uuid, text, uuid, time, uuid) AS INSERT INTO messages VALUES($1, $2, $3, $4, $5);")
    execute_batch(db_con.cursor, """EXECUTE mes (%(id)s, %(text)s, %(category_id)s,
                                              %(posted_at)s, %(author_id)s)""", messages)
    db_con.connection.commit()
    print('DEBUG: inserting messages...')
    db_con.disconnect()
