import sqlite3
import json


class DataBase():
    def __init__(self):
        con = sqlite3.connect("network.db")
        cur = con.cursor()
        self.cur = cur
        self.cur.execute("CREATE TABLE IF NOT EXISTS Users(id_user int, first_name string, last_name string, bdate string, sex int, city string, country string, followers_count int, relation int, political int, people_main int, life_main int, smoking int, alcohol int, Primary key(id_user))")
        print("CREATE TABLE IF NOT EXISTS Users(id_user int, first_name string, last_name string, bdate string, sex int, city string, country string, followers_count int, relation int, political int, people_main int, life_main int, smoking int, alcohol int, Primary key(id_user))")
        self.cur.execute("CREATE TABLE IF NOT EXISTS Friends(id_friendship int, id_first_user int, id_second_user int)")
        print("CREATE TABLE IF NOT EXISTS Friends(id_friendship int, id_first_user int, id_second_user int)")
        self.nrows_users = 0
        self.ncols_users = 14
        self.nrows_friends = 0
        self.ncols_friends = 3

    def load(self, users):
        for i in range(len(users['id'])):
            """
            print(f"INSERT OR IGNORE INTO Users VALUES " +
            f"({users['id'][i]}, '{users['first_name'][i]}', '{users['last_name'][i]}', '{users['bdate'][i]}', " +
            f"{users['sex'][i]}, '{users['city'][i]}', '{users['country'][i]}', {users['followers_count'][i]}, " +
            f"{users['relation'][i]}, {users['political'][i]}, {users['people_main'][i]}, {users['life_main'][i]}, " +
            f"{users['smoking'][i]}, {users['alcohol'][i]})")
            """
            self.cur.execute(f"INSERT OR IGNORE INTO Users VALUES " +
                             f"({users['id'][i]}, '{users['first_name'][i]}', '{users['last_name'][i]}', '{users['bdate'][i]}', " +
                             f"{users['sex'][i]}, '{users['city'][i]}', '{users['country'][i]}', {users['followers_count'][i]}, " +
                             f"{users['relation'][i]}, {users['political'][i]}, {users['people_main'][i]}, {users['life_main'][i]}, " +
                             f"{users['smoking'][i]}, {users['alcohol'][i]})")
        self.cur.execute(f"SELECT * FROM Users")
        self.nrows_users = len(self.cur.fetchall())
        count = 0
        for i, user in enumerate(users['friends'][:50]):  # слишком долго работает, поэтому ограничиваем
            for j, friend in enumerate(user):
                if users['id'][i] < friend:
                    #  self.cur.execute(f"SELECT id_friendship FROM Friends WHERE id_first_user = {users['id'][i]} AND id_second_user = {friend}")
                    #  if len(self.cur.fetchall()) == 0:
                    #  print(f"INSERT OR IGNORE INTO Friends VALUES ({count}, {users['id'][i]}, {friend})")
                    self.cur.execute(f"INSERT OR IGNORE INTO Friends VALUES ({count}, {users['id'][i]}, {friend})")
                    count += 1
                else:
                    #  self.cur.execute(f"SELECT id_friendship FROM Friends WHERE id_first_user = {friend} AND id_second_user = {users['id'][i]}")
                    #  if len(self.cur.fetchall()) == 0:
                    #  print(f"INSERT OR IGNORE INTO Friends VALUES ({count}, {friend}, {users['id'][i]})")
                    self.cur.execute(f"INSERT OR IGNORE INTO Friends VALUES ({count}, {friend}, {users['id'][i]})")
                    count += 1
        self.cur.execute(f"SELECT * FROM Friends")
        self.nrows_friends = len(self.cur.fetchall())
        return self

    def size(self, table):
        if table == 'Users':
            return (self.nrows_users, self.ncols_users)
        elif table == 'Friends':
            return (self.nrows_friends, self.ncols_friends)
        else:
            raise ValueError("There is no such table")

    def add(self, table, values, variables=''):
        if table == 'Users' or table == 'Friends':
            if table == 'Users':
                if variables:
                    print(f"INSERT INTO {table} ({variables}) VALUES ({values})")
                    self.cur.execute(f"INSERT INTO {table} ({variables}) VALUES ({values})")
                else:
                    print(f"INSERT INTO {table} VALUES ({values})")
                    self.cur.execute(f"INSERT INTO {table} VALUES ({values})")
                self.nrows_users += 1
            else:
                if variables:
                    print(f"INSERT INTO {table} (id_friendship, {variables}) VALUES ({self.nrows_friends}, {values})")
                    self.cur.execute(f"INSERT INTO {table} (id_friendship, {variables}) VALUES ({self.nrows_friends}, {values})")
                else:
                    print(f"INSERT INTO {table} VALUES ({self.nrows_friends}, {values})")
                    self.cur.execute(f"INSERT INTO {table} VALUES ({self.nrows_friends}, {values})")
                self.nrows_friends += 1
        else:
            raise ValueError("There is no such table")

    def print(self, table, values, condition='', distinct='', order_by=''):
        if table == 'Users' or table == 'Friends' or table == 'Users, Friends' or table == 'Friends, Users':
            if condition:
                print(f"SELECT {distinct} {values} FROM {table} WHERE {condition}" + f" ORDER BY {order_by}"*int(bool(order_by)))
                self.cur.execute(f"SELECT {distinct} {values} FROM {table} WHERE {condition}" + f" ORDER BY {order_by}"*int(bool(order_by)))
            else:
                print(f"SELECT {distinct} {values} FROM {table}" + f" ORDER BY {order_by}"*int(bool(order_by)))
                self.cur.execute(f"SELECT {distinct} {values} FROM {table}" + f" ORDER BY {order_by}"*int(bool(order_by)))
            print(self.cur.fetchall())
        else:
            raise ValueError("There is no such table")

    def delete(self, table, condition=''):
        if table == 'Users' or table == 'Friends' or table == 'Users, Friends' or table == 'Friends, Users':
            if condition:
                self.cur.execute(f"SELECT * FROM {table} WHERE {condition}")
            else:
                self.cur.execute(f"SELECT * FROM {table}")
            num = len(self.cur.fetchall())
            if num:
                if condition:
                    print(f"DELETE FROM {table} WHERE {condition}")
                    self.cur.execute(f"DELETE FROM {table} WHERE {condition}")
                else:
                    print(f"DELETE FROM {table}")
                    self.cur.execute(f"DELETE FROM {table}")
                if table == 'Users':
                    self.nrows_users -= num
                else:
                    self.nrows_friends -= num
        else:
            raise ValueError("There is no such table")

    def update(self, table, statement, condition=''):
        if table == 'Users' or table == 'Friends':
            if condition:
                print(f"UPDATE {table} SET {statement} WHERE {condition}")
                self.cur.execute(f"UPDATE {table} SET {statement} WHERE {condition}")
            else:
                print(f"UPDATE {table} SET {statement}")
                self.cur.execute(f"UPDATE {table} SET {statement}")
        else:
            raise ValueError("There is no such table")


def dialog(db):
    print('----------------------------------------')
    print("What do you want to do?")
    print("Choose from the following options:")
    print("0 - finish the dialog")
    print("1 - Add data")
    print("2 - Print data")
    print("3 - Delete data")
    print("4 - Update data")
    print("5 - Find size of a table")
    option = int(input("Paste a necessary option: ").strip())
    if option == 0:
        return
    while option not in range(0, 6):
        print("There is no such option. Try again!")
        option = int(input('Paste a necessary option: ').strip())
    table = input('Please, enter the name of the table: ').strip()
    if option == 0:
        return
    if option == 1:
        values = input('Enter all the values for your request: ').strip()
        variables = input('Enter all the variables for your request or just press enter: ').strip()
        db.add(table, values, variables)
    elif option == 2:
        values = input('Enter all the values for your request: ').strip()
        condition = input('Enter all the conditions for your request or just press enter: ').strip()
        distinct = input('Enter \'distinct\' or just press enter for your request: ').strip()
        order_by = input('Enter the type of ordering or just press enter for your request: ').strip()
        db.print(table, values, condition, distinct, order_by)
    elif option == 3:
        condition = input('Enter a condition for rows that you want to delete or just press enter: ').strip()
        db.delete(table, condition)
    elif option == 4:
        statement = input('Enter a statement that you want to add: ')
        condition = input('Enter a condition for rows that you want to update or just press enter: ').strip()
        db.update(table, statement, condition)
    elif option == 5:
        print(db.size(table))
    exit = input('If you want to exit, print "exit". Otherwise print anything else: ').strip()
    if exit == 'exit':
        return
    dialog(db)


def main():
    db = DataBase()
    with open('data.json', 'r', encoding='utf-8') as f:
        users = json.load(f)
    db = db.load(users)
    """
    print(db.size('Users'))
    print(db.size('Friends'))
    try:
        print(db.size('friends'))
    except ValueError:
        pass
    print(db.size('Users'))
    db.add('Users', '\'Eduard\', \'Grigoryev\'', 'first_name, last_name')
    print(db.size('Users'))
    db.add('Users', '13, \'Eduard\', \'Grigoryev\', \'1996.10.11\', 2, \'Москва\', \'Россия\', 0, 1, 4, 2, 6, 1, 1')
    db.print('Users', '*', 'id_user = 13')
    print(db.size('Users'))
    db.delete('Friends', 'id_first_user = 1 and id_second_user = 2')
    print(db.size('Friends'))
    db.print('Users', 'first_name, last_name', condition='political = 9', distinct='distinct', order_by='id_user ASC')
    db.print('Users', 'first_name, last_name', condition='political = 9', order_by='id_user DESC')
    db.update('Users', 'first_name=\'Edik\', last_name=\'Grigoriev\'', 'id_user = 13')
    db.print('Users', 'first_name, last_name, bdate', condition='political=9', order_by='bdate DESC')
    n = 912
    db.print('Friends, Users', 'Users.first_name', condition=f"Friends.id_first_user = {n} OR Friends.id_second_user = {n}", distinct='distinct')
    db.update('Users', 'first_name=\'Edik\', last_name=\'Grigoriev\'')
    db.print('Users', 'first_name, last_name, bdate', condition='political=9', order_by='bdate DESC')
    db.delete('Users', 'political=9')
    print(db.size('Users'))
    db.delete('Users')
    print(db.size('Users'))
    db.delete('Friends')
    print(db.size('Friends'))
    """
    dialog(db)


if __name__ == "__main__":
    main()
