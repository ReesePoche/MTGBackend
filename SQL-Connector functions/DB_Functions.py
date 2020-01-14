"""
The following will take data in excel files created by the webScrapper and put them in the SQL database that is set up
with the SQL statements found in the file MTG_DB_SETUP.sql

It is split into three sections.
The first is the code that will parse the data from the excel files.
The Second are the functions that use mysql.connector to add the data to the database.
The third is just one line of code and is the code that runs functions from the above sections to add the data to the DB.

Need to pip mysql-connector-python made by Oracle, pandas, and  (numpy)<<<<< in your python environment.
"""

import mysql.connector
import pandas as pd
import os

"""
First Section
    parses data from excel files
    Add a value to the path_to_excel_files_folder to where the excel files are. 
    Change value of show_card_data_as_its_added to True if you want to see console output of data being added for testing.
    If you are running this on a linux environment you will have to change \\ in line 28 to /.
"""
path_to_excel_files_folder = ""
show_card_data_as_its_added = False
def piper(table):
    df = pd.read_excel(table)
    splitlip = table.split(('\\'))
    target_values = splitlip[-1]
    target_values = target_values[:-5]
    date = target_values[-10:]
    target_values = target_values[:-10]
    target_values = target_values.strip('_')
    another_list = target_values.split('_')
    set_name = ""
    for each in another_list:
        set_name += (each + " ")
    set_name = set_name[:-1]
    for index, row in df.iterrows():
        Card_Name = row['Name']
        Card_Name = Card_Name.replace("'","").replace(",",'')
        rarity = row['Rarity']
        if(rarity == 'C'): rarity = 'common'
        elif rarity == 'U': rarity = 'uncommon'
        elif rarity == 'R': rarity = 'rare'
        elif rarity == 'M': rarity = 'mythic'
        else: continue #this will get rid of all land card and token card types. DB was not designed to contain them.
        Market_Price = row['Market Price']
        Buy_List = row['Buy List Price']
        Listed_Median = row['Listed Median']
        if show_card_data_as_its_added:
            print("about to add the following card data into the DB\n")
            print(Card_Name)
            print(date)
            print(rarity)
        if add_card_price_data(Card_Name, rarity, 'Throne Of Eldraine', date, Market_Price, Buy_List, Listed_Median):
            print("card data successfully added to database\n")
        else:
            print("card data failed to be added")


def do_the_loop(path = path_to_excel_files_folder):
    """
    Goes through folder and calls piper function on every excel file to add data to DataBase
    :param path: the path to the folder that contains the excel files that you want to parse
    :return:
    """
    listerman = os.listdir(path)
    for each in listerman:
        if each.__contains__('.xlsx'):
            piper("{}\\{}".format(path,each))

"""
Second Section
    These are a series of functions that add data to the Database Using mysql-connector package
    Assumes you are using DB made by  MTG_DB_SETUP.sql statements (see MTG_DB_SETUP.sql for full database details)
    Please fill out creditials for the DB below. 
    Please add variable for port number if SQL DB port used is not the default. 
"""
HOST_TO_USE = 'localhost' #assume you are locally connected to DB
DATABASE_TO_USE = 'group9' #name of DB given by MTG_DB_SETUP.sql
USERNAME_TO_USE='root'
PASSWORD_TO_USE=''

def get_card_id(card_name):
    """
    uses sql query
    SELECT card_id
        FROM group9.Cards as C
        WHERE C.card_name = card_name
    :param card_name: the name of the card you want to find the card_id for
    :return: will return an int that is the card_id of the given card. will return 0 if card is not found -1 if connection error
    """
    try:
        query = "SELECT C.card_id FROM Cards as C WHERE C.card_name='" + card_name + "';"

        connection = mysql.connector.connect(host=HOST_TO_USE,
                                             database=DATABASE_TO_USE,
                                             user=USERNAME_TO_USE,
                                             password=PASSWORD_TO_USE)
        cursor = connection.cursor()

        cursor.execute(query)
        result = cursor.fetchone()
        if result is None:
            return 0
        else:
            return result[0]
    except mysql.connector.Error as error:
        print("parameterized query failed  at add  get card id ... {}".format(error))
        return -1
    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()

def get_set_id(set_name):
    """
    :param set_name: the name of the set you want to get the id for
    :return: an int that is the set_id of the set with the given name. will return 0 if set does not exist -1 if connection error
    """
    try:
        query = "SELECT S.set_id FROM Sets as S WHERE S.set_name='" + set_name + "';"
        connection = mysql.connector.connect(host=HOST_TO_USE,
                                             database=DATABASE_TO_USE,
                                             user=USERNAME_TO_USE,
                                             password=PASSWORD_TO_USE)
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        if result is None:
            return 0
        else:
            return result[0]
    except mysql.connector.Error as error:
        print("parameterized query failed at get set id {}".format(error))
        return -1
    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()


def add_card(card_name, rarity, set_name):
    """
    Adds the card with the given parameters to the DB
    :param card_name: Name of card to be added to DB
    :param rarity: rarity of card to be added to DB
    :param set_name: Name of set of card to be added to DB
    :return: returns the auto generated Card ID of the card being added. Will be 0 if card was not added -1 if connection failure occured
    """
    setid = get_set_id(set_name)
    try:
        query = "INSERT INTO Cards (card_name, rarity, set_id) VALUES (%s, %s, %s);"
        data = (card_name, rarity, setid)
        connection = mysql.connector.connect(host=HOST_TO_USE,
                                             database=DATABASE_TO_USE,
                                             user=USERNAME_TO_USE,
                                             password=PASSWORD_TO_USE)
        cursor = connection.cursor(prepared=True)
        cursor.execute(query, data)
        connection.commit()
    except mysql.connector.Error as error:
        print("parameterized query failed at add card {}".format(error))
    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
    return get_card_id(card_name)

def add_card_price_data(card_name, rarity, set_name, pull_date, market_price, buylist_price, listed_median):
    """
    Main fuction that will be used for adding Data to the DB
        Gets cardID of card with given card name. If card does not exist in DB will add it to DB
        After getting the Card ID, function then adds the gathered data into the DB.
    :param card_name: Name of card to be added
    :param rarity: rarity of card to be added
    :param set_name: the name of the set of the card to be added
    :param pull_date: the date of the card data
    :param market_price: The market price of the card on the given date
    :param buylist_price: the buylist price of the card on the given date
    :param listed_median: The listed median price of the card on the given date
    :return:
    """
    cardid = get_card_id(card_name)
    while(cardid==0):
        cardid = add_card(card_name, rarity, set_name)
    try:
        query = "INSERT INTO Card_Stocks (card_id, pull_date, market_price, buylist_price, listed_median) VALUES (%s, %s, %s, %s, %s);"
        data = (cardid, pull_date, market_price, buylist_price, listed_median)
        connection = mysql.connector.connect(host=HOST_TO_USE,
                                             database=DATABASE_TO_USE,
                                             user=USERNAME_TO_USE,
                                             password=PASSWORD_TO_USE)
        cursor = connection.cursor(prepared=True)
        cursor.execute(query, data)
        connection.commit()
        return True
    except mysql.connector.Error as error:
        print("parameterized query failed at add card stock data{}".format(error))
        return False
    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()

"""
Third Section
    Just runs the function do_the_loop() no parameters needed if path_to_excel_files_folder is the path desired.
"""

do_the_loop()

