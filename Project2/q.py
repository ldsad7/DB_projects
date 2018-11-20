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

        sql_request = "CREATE DATABASE IF NOT EXISTS RecipesDB;"
        self.cur.execute(sql_request)
        sql_request = "USE RecipesDB;"
        self.cur.execute(sql_request)

        with open('products.json', 'r', encoding='utf-8') as f:
            self.products = json.load(f)

        with open('recipes.json', 'r', encoding='utf-8') as f:
            self.recipes = json.load(f)

        sql_request = "CREATE TABLE IF NOT EXISTS Recipes(id_recipe int, name VARCHAR(100), Primary key(id_recipe));"
        print(sql_request)
        self.cur.execute(sql_request)

        sql_request = "CREATE TABLE IF NOT EXISTS Products(id_product int, name VARCHAR(100), calories float);" # number of calories in 100 grams
        print(sql_request)
        self.cur.execute(sql_request)

        sql_request = "CREATE TABLE IF NOT EXISTS Recipes_and_Products(id_recipe int, id_product int, grams_product float);"
        print(sql_request)
        self.cur.execute(sql_request)

        self.nrows_recipes = 0
        self.ncols_recipes = 2 # including the primary key
        self.nrows_products = 0
        self.ncols_products = 3
        self.nrows_recipes_and_products = 0
        self.ncols_recipes_and_products = 3

    def __del__(self):
        self.cur.close()
        self.con.close()

    def load_data(self):
        count = 0
        for key, value in self.recipes.items():
            sql_request = f"INSERT INTO Recipes VALUES ({count}, '{key}')"
            # print(sql_request)
            self.cur.execute(sql_request)
            count += 1

        count = 0
        for key, value in self.products.items():
            sql_request = f"INSERT INTO Products VALUES ({count}, '{key}', {value})"
            # print(sql_request)
            self.cur.execute(sql_request)
            count += 1

        id_recipe = 0
        for key, value in self.recipes.items():
            for inner_key, inner_value in value.items():
                try:
                    id_product = list(self.products.keys()).index(inner_key.capitalize())
                except ValueError: # There is no such an ingredient in the database
                    continue
                sql_request = f"INSERT INTO Recipes_and_Products VALUES ({id_recipe}, {id_product}, {inner_value})"
                # print(sql_request)
                self.cur.execute(sql_request)
            id_recipe += 1

        self.cur.execute(f"SELECT * FROM Products")
        self.nrows_products = len(self.cur.fetchall())
        self.cur.execute(f"SELECT * FROM Recipes")
        self.nrows_recipes = len(self.cur.fetchall())
        self.cur.execute(f"SELECT * FROM Recipes_and_Products")
        self.nrows_recipes_and_products = len(self.cur.fetchall())

        return self

    def size(self, table):
        if table.lower().strip() == 'products':
            return (self.nrows_products, self.ncols_products)
        elif table.lower().strip() == 'recipes':
            return (self.nrows_recipes, self.ncols_recipes)
        elif table.lower().strip() == 'recipes_and_products':
            return (self.nrows_recipes_and_products, self.ncols_recipes_and_products)
        else:
            raise ValueError("There is no such table")

    def add(self, table, values, variables=''):
        if table.lower().strip() in ['products', 'recipes', 'recipes_and_products']:
            if table.lower().strip() in 'recipes':
                if variables:
                    sql_request = f"INSERT INTO {table} (id_recipe, {variables}) VALUES ({self.nrows_recipes}, {values})"
                else:
                    sql_request = f"INSERT INTO {table} VALUES ({self.nrows_recipes}, {values})"
                print(sql_request)
                self.cur.execute(sql_request)
                self.nrows_recipes += 1
            elif table.lower().strip() == 'products':
                if variables:
                    sql_request = f"INSERT INTO {table} (id_product, {variables}) VALUES ({self.nrows_products}, {values})"
                else:
                    sql_request = f"INSERT INTO {table} VALUES ({self.nrows_products}, {values})"
                print(sql_request)
                self.cur.execute(sql_request)
                self.nrows_products += 1
            else:
                if variables:
                    sql_request = f"INSERT INTO {table} ({variables}) VALUES ({values})"
                else:
                    sql_request = f"INSERT INTO {table} VALUES ({values})"
                print(sql_request)
                self.cur.execute(sql_request)
                self.nrows_recipes_and_products += 1
        else:
            raise ValueError("There is no such a table")

    def print(self, table, values, condition='', distinct='', order_by='', order_by_column=''):
        if not (set(map(str.strip, table.lower().strip().split(','))) - {'products', 'recipes', 'recipes_and_products'}):
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
        if table.lower().strip() in ['products', 'recipes', 'recipes_and_products']:
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
                if 'products' in set(map(str.strip, table.lower().strip().split(','))):
                    self.nrows_products -= num
                if 'recipes' in set(map(str.strip, table.lower().strip().split(','))):
                    self.nrows_recipes -= num
                if 'recipes_and_products' in set(map(str.strip, table.lower().strip().split(','))):
                    self.nrows_recipes_and_products -= num
        else:
            raise ValueError("There is no such table")

    def update(self, table, statement, condition=''):
        if table.lower().strip() in ['products', 'recipes', 'recipes_and_products']:
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

    def calories(self, recipes):
        result = []
        for recipe in recipes:
            recipe = recipe.strip()
            sql_request = f"SELECT id_recipe FROM recipes WHERE recipes.name='{recipe.lower()}'"
            print(sql_request)
            self.cur.execute(sql_request)
            id_recipe = self.cur.fetchall()[0][0]
            sql_request = f"SELECT id_product, grams_product FROM recipes_and_products WHERE id_recipe={id_recipe}"
            print(sql_request)
            self.cur.execute(sql_request)
            prods = self.cur.fetchall()
            sum_ = 0
            for prod in prods:
                sql_request = f"SELECT calories from Products WHERE products.id_product={prod[0]}"
                print(sql_request)
                self.cur.execute(sql_request)
                cals = self.cur.fetchall()[0][0]
                sum_ += cals * (prod[1] / 100)
            result.append(sum_)
        return result

    def perform(self, sql_request):
        self.cur.execute(sql_request)
        print(sql_request)
        result = self.cur.fetchall()
        print(result)

def dialog(db):
    print('----------------------------------------')
    print("What do you want to do?")
    print("Choose from the following options:")
    print("0 - finish the dialog")
    print("1 - Add data")
    print("2 - Print data")
    print("3 - Delete data")
    print("4 - Update data")
    print("5 - Find the size of a table")
    print("6 - Describe a table")
    print("7 - Show tables")
    print("8 - Find out the calorific value of specific recipes")
    print("9 - Write my own SQL request")

    option = int(input("Paste a necessary option: ").strip())
    if option == 0:
        return

    while option not in range(0, 10):
        print("There is no such option. Try again!")
        option = int(input('Paste a necessary option: ').strip())
    if option in range(0, 7):
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
        order_by_column = input('Enter the column of ordering or just press enter for your request: ').strip()
        db.print(table, values, condition, distinct, order_by, order_by_column)
    elif option == 3:
        condition = input('Enter a condition for rows that you want to delete or just press enter: ').strip()
        db.delete(table, condition)
    elif option == 4:
        statement = input('Enter a statement that you want to add: ')
        condition = input('Enter a condition for rows that you want to update or just press enter: ').strip()
        db.update(table, statement, condition)
    elif option == 5:
        print(db.size(table))
    elif option == 6:
        db.describe(table)
    elif option == 7:
        db.show()
    elif option == 8:
        recipes = input('Enter comma separated the recipes for which you want to know the calorific value: ').split(',')
        results = db.calories(recipes)
        for i in range(len(results)):
            print(recipes[i].strip() + ':', round(results[i], 2))
    elif option == 9:
        sql_request = input('Enter an sql request that you want to perform: ')
        db.perform(sql_request)

    exit = input('If you want to exit, print "exit". Otherwise print anything else: ').strip()
    if exit == 'exit':
        return
    dialog(db)


def main():
    db = DataBase()
    db = db.load_data()

    """
    print(db.size('Products'))
    print(db.size('Recipes'))
    print(db.size('recipes'))
    print(db.size('Recipes_and_Products'))
    db.add('products', '\'саподилла\', 350')
    db.add('products', '\'рамбутан\'', 'name')
    print(db.size('products'))
    db.add('recipes', '\'Экзотический десерт\'')
    print(db.size('recipes'))
    db.add('recipes_and_products', '12, 35, 250')
    print(db.size('recipes_and_products'))
    db.print('recipes_and_products', '*', 'id_recipe = 12')
    db.print('recipes_and_products', 'id_recipe, id_product', 'id_recipe = 12', order_by="ASC", order_by_column="id_product")
    db.print('recipes_and_products', 'id_recipe, id_product', 'id_product = 496', distinct="distinct", order_by="DESC", order_by_column="id_recipe")
    db.delete('recipes_and_products', 'recipes_and_products.id_product=496')
    print(db.size('recipes_and_products'))
    db.print('recipes_and_products', 'id_recipe, id_product', 'id_recipe=1')
    db.update('recipes_and_products', 'id_product=111', 'id_recipe=1 AND id_product=20')
    db.print('recipes_and_products', 'id_recipe, id_product', 'id_recipe=1')
    print(db.calories('А-ля крюшон, А-ля рататуй, А-ля рыбные бутерброды'.split(',')))
    print(db.calories(['Жаркое в горшочке с курицей']))
    db.delete('recipes')
    print(db.size('recipes'))
    
    # From here follow CoMpLeX ReQuEsTs:

    db.perform("SELECT \
                recipes.name AS recipe_name, \
                recipes_and_products.grams_product AS product_in_grams \
                FROM recipes \
                INNER JOIN recipes_and_products \
                ON recipes.id_recipe = recipes_and_products.id_recipe \
                WHERE recipes.id_recipe = 1")

    db.perform("SELECT r.name \"recipe_name\", p.name \"product_name\" \
                FROM recipes_and_products rap, recipes r, products p \
                WHERE r.id_recipe=rap.id_recipe AND p.id_product=rap.id_product")

    n = 1 # number of the recipe
    db.print('recipes', 'name', f"id_recipe={n}") # name of the recipe
    db.perform(f"SELECT p.name \"product_name\" \
                FROM recipes_and_products rap, recipes r, products p \
                WHERE r.id_recipe=rap.id_recipe AND p.id_product=rap.id_product AND r.id_recipe={n}") # ingredients of this recipe
    
    n = 1978 # number of the product
    db.print('products', 'name', f"id_product={n}") # name of the product
    db.perform(f"SELECT r.name recipe_name \
                FROM recipes_and_products rap, recipes r, products p \
                WHERE r.id_recipe=rap.id_recipe AND p.id_product=rap.id_product AND p.id_product={n}") # names of the recipes that contain it
    """

    dialog(db)

if __name__ == '__main__':
    main()