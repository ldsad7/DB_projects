import mysql.connector
import json
import re

class DataBase():
    def __init__(self):
        self.con = mysql.connector.connect(
            auth_plugin="mysql_native_password",
            host="localhost",
            user="user",
            passwd="password"
        )

        self.cur = self.con.cursor()

        # sql_request = "DROP DATABASE IF EXISTS massmedia;"
        # print(sql_request)
        # self.cur.execute(sql_request)
        
        sql_request = "CREATE DATABASE IF NOT EXISTS massmedia;"
        # print(sql_request)
        self.cur.execute(sql_request)
        sql_request = "USE massmedia;"
        # print(sql_request)
        self.cur.execute(sql_request)

        with open('articles.json', 'r', encoding='utf-8') as f:
            self.articles = json.load(f)

        with open('meta.json', 'r', encoding='utf-8') as f:
            self.meta = json.load(f)

        with open('info.json', 'r', encoding='utf-8') as f:
            self.info = json.load(f)

        sql_request = "CREATE TABLE IF NOT EXISTS Languages(id_language int, name VARCHAR(100), Primary key(id_language));"
        # print(sql_request)
        self.cur.execute(sql_request)

        sql_request = "CREATE TABLE IF NOT EXISTS Countries(id_country int, name VARCHAR(100), Primary key(id_country));"
        # print(sql_request)
        self.cur.execute(sql_request)

        sql_request = "CREATE TABLE IF NOT EXISTS Media(id_media int, name VARCHAR(100), site VARCHAR(100), id_language int, id_country int, Primary key(id_media));"
        # print(sql_request)
        self.cur.execute(sql_request)

        sql_request = "CREATE TABLE IF NOT EXISTS Articles(id_article int, name VARCHAR(100), text LONGTEXT, Primary key(id_article));"
        # print(sql_request)
        self.cur.execute(sql_request)

        sql_request = "CREATE TABLE IF NOT EXISTS Meta(id_meta int, name VARCHAR(100), Primary key(id_meta));"
        # print(sql_request)
        self.cur.execute(sql_request)

        sql_request = "CREATE TABLE IF NOT EXISTS Articles_to_Media(id_article int, id_media int);"
        # print(sql_request)
        self.cur.execute(sql_request)

        sql_request = "CREATE TABLE IF NOT EXISTS Articles_to_Meta(id_article int, id_meta int, meta_text VARCHAR(200));"
        # print(sql_request)
        self.cur.execute(sql_request)

        self.nrows_languages = 0
        self.ncols_languages = 2
        self.nrows_countries = 0
        self.ncols_countries = 2
        self.nrows_media = 0
        self.ncols_media = 5
        self.nrows_articles = 0
        self.ncols_articles = 3
        self.nrows_meta = 0
        self.ncols_meta = 2
        self.nrows_articles_to_media = 0
        self.ncols_articles_to_media = 2
        self.nrows_articles_to_meta = 0
        self.ncols_articles_to_meta = 3

    def __del__(self):
        self.cur.close()
        self.con.close()

    def load_data(self):
        sites = []
        names = []
        langs = []
        countries = []
        for key, value in self.info.items():
            sites.append(key)
            names.append(value[0])
            langs.append(value[1])
            countries.append(value[2])

        for count, lang in enumerate(sorted(list(set(langs)))):
            sql_request = f"INSERT INTO Languages VALUES ({count}, '{lang}')"
            # print(sql_request)
            self.cur.execute(sql_request)

        for count, country in enumerate(sorted(list(set(countries)))):
            sql_request = f"INSERT INTO Countries VALUES ({count}, '{country}')"
            # print(sql_request)
            self.cur.execute(sql_request)

        for count, site in enumerate(sites):
            sql_request = f"SELECT id_language FROM Languages WHERE name='{langs[count]}'"
            self.cur.execute(sql_request)
            id_lang = self.cur.fetchall()[0][0]
            
            sql_request = f"SELECT id_country FROM Countries WHERE name='{countries[count]}'"
            self.cur.execute(sql_request)
            id_country = self.cur.fetchall()[0][0]
            
            sql_request = f"INSERT INTO Media VALUES ({count}, '{names[count]}', '{site}', {id_lang}, {id_country})"
            # print(sql_request)
            self.cur.execute(sql_request)

        count_meta = 0
        count_articles = -1
        unique = set()
        for value in self.meta.values():
            for dct in value:
                count_articles += 1
                for inner_key, inner_value in dct.items():
                    if len(inner_value) > 100:
                        continue
                    if inner_key not in unique:
                        sql_request = f"INSERT INTO Meta VALUES ({count_meta}, '{inner_key}')"
                        # print(sql_request)
                        self.cur.execute(sql_request)
                        unique |= {inner_key}
                        count_meta += 1

                    sql_request = f"SELECT id_meta FROM Meta WHERE name='{inner_key}'"
                    self.cur.execute(sql_request)
                    id_meta = self.cur.fetchall()[0][0]
                    inner_value = re.sub('\'', '\\\'', inner_value, 0)
                    sql_request = f"INSERT INTO Articles_to_Meta VALUES ({count_articles}, {id_meta}, '{inner_value}')"
                    # print(sql_request)
                    self.cur.execute(sql_request)

        count = 0
        for key, value in self.articles.items():
            sql_request = f"SELECT id_media FROM Media WHERE site='{key}'"
            self.cur.execute(sql_request)
            id_media = self.cur.fetchall()[0][0]
            for elem in value:
                elem = re.sub('\'', '\\\'', elem, 0)
                if len(elem) > 10000:
                    elem = elem[:10000]
                sql_request = f"INSERT INTO Articles VALUES ({count}, '...', '{elem}')"
                # print(sql_request)
                self.cur.execute(sql_request)

                sql_request = f"INSERT INTO Articles_to_Media VALUES ({count}, {id_media})"
                # print(sql_request)
                self.cur.execute(sql_request)

                count += 1

        self.cur.execute(f"SELECT * FROM Languages")
        self.nrows_languages = len(self.cur.fetchall())
        self.cur.execute(f"SELECT * FROM Countries")
        self.nrows_countries = len(self.cur.fetchall())
        self.cur.execute(f"SELECT * FROM Media")
        self.nrows_media = len(self.cur.fetchall())
        self.cur.execute(f"SELECT * FROM Articles")
        self.nrows_articles = len(self.cur.fetchall())
        self.cur.execute(f"SELECT * FROM Meta")
        self.nrows_meta = len(self.cur.fetchall())
        self.cur.execute(f"SELECT * FROM Articles_to_Media")
        self.nrows_articles_to_media = len(self.cur.fetchall())
        self.cur.execute(f"SELECT * FROM Articles_to_Meta")
        self.nrows_articles_to_meta = len(self.cur.fetchall())

        return self

    def size(self, table):
        if table.lower().strip() == 'languages':
            return (self.nrows_languages, self.ncols_languages)
        elif table.lower().strip() == 'countries':
            return (self.nrows_countries, self.ncols_countries)
        elif table.lower().strip() == 'media':
            return (self.nrows_media, self.ncols_media)
        elif table.lower().strip() == 'articles':
            return (self.nrows_articles, self.ncols_articles)
        elif table.lower().strip() == 'meta':
            return (self.nrows_meta, self.ncols_meta)
        elif table.lower().strip() == 'articles_to_meta':
            return (self.nrows_articles_to_meta, self.ncols_articles_to_meta)
        elif table.lower().strip() == 'articles_to_media':
            return (self.nrows_articles_to_media, self.ncols_articles_to_media)
        else:
            raise ValueError("There is no such table")

    def add(self, table, values, variables=''):
        if table.lower().strip() in ['articles', 'articles_to_media', 'articles_to_meta', 'countries', 'languages', 'media', 'meta']:
            if table.lower().strip() == 'articles':
                if variables:
                    sql_request = f"INSERT INTO {table} (id_article, {variables}) VALUES ({self.nrows_articles}, {values})"
                else:
                    sql_request = f"INSERT INTO {table} VALUES ({self.nrows_articles}, {values})"
                print(sql_request)
                self.cur.execute(sql_request)
                self.nrows_articles += 1
            elif table.lower().strip() in ['articles_to_media', 'articles_to_meta']:
                if variables:
                    sql_request = f"INSERT INTO {table} ({variables}) VALUES ({values})"
                else:
                    sql_request = f"INSERT INTO {table} VALUES ({values})"
                print(sql_request)
                self.cur.execute(sql_request)
                if table.lower().strip() == 'articles_to_media':
                    self.nrows_articles_to_media += 1
                else:
                    self.nrows_articles_to_meta += 1
            elif table.lower().strip() == 'countries':
                if variables:
                    sql_request = f"INSERT INTO {table} (id_country, {variables}) VALUES ({self.nrows_countries}, {values})"
                else:
                    sql_request = f"INSERT INTO {table} VALUES ({self.nrows_countries}, {values})"
                print(sql_request)
                self.cur.execute(sql_request)
                self.nrows_countries += 1
            elif table.lower().strip() == 'languages':
                if variables:
                    sql_request = f"INSERT INTO {table} (id_language, {variables}) VALUES ({self.nrows_languages}, {values})"
                else:
                    sql_request = f"INSERT INTO {table} VALUES ({self.nrows_languages}, {values})"
                print(sql_request)
                self.cur.execute(sql_request)
                self.nrows_languages += 1
            elif table.lower().strip() == 'media':
                if variables:
                    sql_request = f"INSERT INTO {table} (id_media, {variables}) VALUES ({self.nrows_media}, {values})"
                else:
                    sql_request = f"INSERT INTO {table} VALUES ({self.nrows_media}, {values})"
                print(sql_request)
                self.cur.execute(sql_request)
                self.nrows_media += 1
            elif table.lower().strip() == 'meta':
                if variables:
                    sql_request = f"INSERT INTO {table} (id_meta, {variables}) VALUES ({self.nrows_meta}, {values})"
                else:
                    sql_request = f"INSERT INTO {table} VALUES ({self.nrows_meta}, {values})"
                print(sql_request)
                self.cur.execute(sql_request)
                self.nrows_meta += 1
            else:
                print("?????")
        else:
            raise ValueError("There is no such a table")

    def print(self, table, values, condition='', distinct='', order_by='', order_by_column=''):
        if not (set(map(str.strip, table.lower().strip().split(','))) - {'articles', 'articles_to_media', 'articles_to_meta', 'countries', 'languages', 'media', 'meta'}):
            if condition:
                sql_request = f"SELECT {distinct} {values} FROM {table} WHERE {condition}" + f" ORDER BY {order_by_column} {order_by}"*int(bool(order_by))
            else:
                sql_request = f"SELECT {distinct} {values} FROM {table}" + f" ORDER BY {order_by_column} {order_by}"*int(bool(order_by))
            print(sql_request)
            self.cur.execute(sql_request)
            print(self.cur.fetchall())
        else:
            raise ValueError("There is no such table(s)")

    def delete(self, table, condition=''):
        if table.lower().strip() in ['articles', 'articles_to_media', 'articles_to_meta', 'countries', 'languages', 'media', 'meta']:
            if condition:
                sql_request = f"SELECT * FROM {table} WHERE {condition}"
            else:
                sql_request = f"SELECT * FROM {table}"
            self.cur.execute(sql_request)
            print(sql_request)
            num = len(self.cur.fetchall())
            if num:
                if condition:
                    sql_request = f"DELETE FROM {table} WHERE {condition}"
                else:
                    sql_request = f"DELETE FROM {table}"
                self.cur.execute(sql_request)
                print(sql_request)
                if table.lower().strip() == 'articles':
                    self.nrows_articles -= num
                elif table.lower().strip() == 'articles_to_media':
                    self.nrows_articles_to_media -= num
                elif table.lower().strip() == 'articles_to_meta':
                    self.nrows_articles_to_meta -= num
                elif table.lower().strip() == 'countries':
                    self.nrows_countries -= num
                elif table.lower().strip() == 'languages':
                    self.nrows_languages -= num
                elif table.lower().strip() == 'media':
                    self.nrows_media -= num
                elif table.lower().strip() == 'meta':
                    self.nrows_meta -= num
        else:
            raise ValueError("There is no such table")

    def update(self, table, statement, condition=''):
        if table.lower().strip() in ['articles', 'articles_to_media', 'articles_to_meta', 'countries', 'languages', 'media', 'meta']:
            if condition:
                sql_request = f"UPDATE {table} SET {statement} WHERE {condition}"
            else:
                sql_request = f"UPDATE {table} SET {statement}"
            print(sql_request)
            self.cur.execute(sql_request)
        else:
            raise ValueError("There is no such table")

    def show(self):
        self.cur.execute("show tables;")
        tables = self.cur.fetchall()
        print(tables)

    def describe(self, table):
        self.cur.execute(f"describe {table};")
        table = self.cur.fetchall()
        print(table)

    def mean_length(self, ids_media):
        result = []
        for id_media in ids_media:
            sql_request = f"SELECT id_article FROM articles_to_media WHERE id_media={id_media}"
            # print(sql_request)
            self.cur.execute(sql_request)
            ids_article = self.cur.fetchall()
            sum = 0
            num = 0
            for id_article in ids_article:
                sql_request = f"SELECT text FROM articles WHERE id_article={id_article[0]}"
                # print(sql_request)
                self.cur.execute(sql_request)
                sum += len(self.cur.fetchall()[0][0])
                num += 1
            if num == 0:
                result.append(0)
            else:
                result.append(sum/num)
        return result

    def perform(self, sql_request):
        self.cur.execute(sql_request)
        print(sql_request)
        result = self.cur.fetchall()
        print(result)

    def dialog(self):
        print('----------------------------------------')
        print("What do you want to do?")
        print("Choose from the following options:")
        print("0 - Finish the dialog")
        print("1 - Add data")
        print("2 - Print data")
        print("3 - Delete data")
        print("4 - Update data")
        print("5 - Find the size of a table")
        print("6 - Describe a table")
        print("7 - Show tables")
        print("8 - Print mean lengths of articles for specific mass media")
        print("9 - Write my own SQL request")

        try:
            option = int(input("Paste a necessary option: ").strip())
        except ValueError:
            option = -1
        if option == 0:
            return

        while option not in range(0, 10):
            print("There is no such option. Try again!")
            option = int(input('Paste a necessary option: ').strip())
        if option == 0:
            return
        if option in range(0, 7):
            table = input('Please, enter the name of the table: ').strip()
        if option == 1:
            values = input('Enter all the values for your request: ').strip()
            variables = input('Enter all the variables for your request or just press enter: ').strip()
            self.add(table, values, variables)
        elif option == 2:
            values = input('Enter all the values for your request: ').strip()
            condition = input('Enter all the conditions for your request or just press enter: ').strip()
            distinct = input('Enter \'distinct\' or just press enter for your request: ').strip()
            order_by = input('Enter the type of ordering or just press enter for your request: ').strip()
            order_by_column = input('Enter the column of ordering or just press enter for your request: ').strip()
            self.print(table, values, condition, distinct, order_by, order_by_column)
        elif option == 3:
            condition = input('Enter a condition for rows that you want to delete or just press enter: ').strip()
            self.delete(table, condition)
        elif option == 4:
            statement = input('Enter a statement that you want to add: ')
            condition = input('Enter a condition for rows that you want to update or just press enter: ').strip()
            self.update(table, statement, condition)
        elif option == 5:
            print(db.size(table))
        elif option == 6:
            self.describe(table)
        elif option == 7:
            self.show()
        elif option == 8:
            ids_media = [elem.strip() for elem in input('Enter comma separated ids (or just type "all") of the mass media for which you want to print the mean length of articles: ').split(',')]
            if len(ids_media) == 1 and ids_media[0] == 'all':
                sql_request = f"SELECT id_media FROM Media"
                self.cur.execute(sql_request)
                ids_media = [elem[0] for elem in self.cur.fetchall()]
            results = self.mean_length(ids_media)
            for i in range(len(results)):
                sql_request = f"SELECT name FROM Media WHERE id_media={ids_media[i]}"
                self.cur.execute(sql_request)
                name = self.cur.fetchall()[0][0]
                print(name.strip() + ':', round(results[i], 2))
        elif option == 9:
            sql_request = input('Enter an sql request that you want to perform: ')
            self.perform(sql_request)
        self.dialog()


def main():
    db = DataBase()
    db = db.load_data()

    """
    print(db.size('Articles'))
    print(db.size('media'))
    print(db.size('articles_to_meta'))
    print(db.size('Articles_to_Meta'))
    db.add('articles', '\'new article\', \'Lorem ipsum, ...\'')
    print(db.size('articles'))
    print(db.size('meta'))
    db.add('meta', '\'theme\'')
    print(db.size('meta'))
    print(db.size('languages'))
    db.add('Languages', '\'Russian\'')
    print(db.size('languages'))
    print(db.size('media'))
    db.add('media', '\'http://keys.ru\', 2', 'site, id_language')
    print(db.size('media'))
    db.print('media', '*', 'id_country IS NOT NULL')
    db.print('media', 'name, id_language, site', 'id_country IS NOT NULL', order_by="DESC", order_by_column="site")
    db.add('articles_to_media', '30, 3')
    db.add('articles_to_media', '30, 3')
    db.print('articles_to_media', 'id_article, id_media', distinct="distinct", order_by="DESC", order_by_column="id_media")
    db.print('languages', '*')
    db.delete('languages', 'id_language=0')
    db.print('languages', '*')
    print(db.size('languages'))
    db.print('media', 'name, site, id_media', 'id_media=0')
    db.update('media', 'name=\'REUTERS\'', 'id_media=0 AND id_language=0')
    db.print('media', 'name, site, id_media', 'id_media=0')
    db.describe('articles_to_meta')
    db.show()
    
    ids_media = [0, 1, 3]
    results = db.mean_length(ids_media)
    for i in range(len(results)):
        sql_request = f"SELECT name FROM Media WHERE id_media={ids_media[i]}"
        db.cur.execute(sql_request)
        name = db.cur.fetchall()[0][0]
        print(name.strip() + ':', round(results[i], 2))
    db.delete('countries')
    print(db.size('countries'))
    """

    # From here follow CoMpLeX ReQuEsTs:

    """
    db.perform("SELECT \
                media.site AS media_site, \
                articles.id_article AS article_id \
                FROM media \
                INNER JOIN articles_to_media \
                ON media.id_media = articles_to_media.id_media \
                INNER JOIN articles \
                ON articles_to_media.id_article = articles.id_article \
                WHERE media.id_language = 0 ORDER BY media_site DESC")
    
    db.perform("SELECT m.name \"meta_name\", atm.meta_text \"article_to_meta_text\" \
                FROM articles_to_meta atm, articles a, meta m \
                WHERE a.id_article=atm.id_article AND m.id_meta=atm.id_meta")
    
    id = 2 # id of the article
    db.print('articles', 'text', f"id_article={id}") # text of the article
    db.perform(f"SELECT md.name \"media_name\", a.id_article \"article_id\", mt.name \"meta_name\", atmt.meta_text \"meta_text\" \
                FROM media md, articles_to_media atmd, articles_to_meta atmt, articles a, meta mt \
                WHERE md.id_media=atmd.id_media AND atmd.id_article=a.id_article AND a.id_article=atmt.id_article AND atmt.id_article={id}")
    
    db.print('languages', 'name')
    db.print('countries', 'name')
    db.perform(f"SELECT DISTINCT l.name, c.name \
                FROM languages l, countries c, media m \
                WHERE c.id_country=m.id_country AND m.id_language=l.id_language")
    """
    db.dialog()

if __name__ == '__main__':
    main()