import json             # Package om .json files in te laden (bvb kolomnamen zijn zo opgeslagen)
import getpass          # Package om een paswoordveldje te genereren.
import mysql.connector  # MySQL package
import numpy as np
import os
import pandas as pd     # Populaire package voor data-verwerking
import sys


def verbind_met_GB(username, hostname, gegevensbanknaam):
    # """
    # Maak verbinding met een externe gegevensbank
    #
    # :param  username:          username van de gebruiker, string
    # :param  hostname:          naam van de host, string.
    #                            Dit is in het geval van een lokale server gewoon 'localhost'
    # :param  gegevensbanknaam:  naam van de gegevensbank, string.
    # :return connection:        connection object, dit is wat teruggeven wordt
    #                            door connect() methods van packages die voldoen aan de DB-API
    # """

    password = "Visserij2"  # Genereer vakje voor wachtwoord in te geven

    connection = mysql.connector.connect(host=hostname,
                                         user=username,
                                         passwd=password,
                                         db=gegevensbanknaam)
    return connection


def run_query(connection, query):
    # """
    # Voer een query uit op een reeds gemaakte connectie, geeft het resultaat van de query terug
    # """

    # Making a cursor and executing the query
    cursor = connection.cursor()
    cursor.execute(query)

    # Collecting the result and casting it in a pd.DataFrame
    res = cursor.fetchall()

    return res


def res_to_df(query_result, column_names):
    # """
    # Giet het resultaat van een uitgevoerde query in een 'pandas dataframe'
    # met vooraf gespecifieerde kolomnamen.
    #
    # Let op: Het resultaat van de query moet dus exact evenveel kolommen bevatten
    # als kolomnamen die je meegeeft. Als dit niet het geval is, is dit een indicatie
    # dat je oplossing fout is. (Gezien wij de kolomnamen van de oplossing al cadeau doen)
    #
    # """
    df = pd.DataFrame(query_result, columns=column_names)
    return df


# Load column names
# Make sure files are in same directory as code!
filename = os.path.join(os.path.dirname(os.getcwd()), 'solution', 'all_q_colnam.json')
col_names = json.load(open(filename, 'r'))

# make connection to database
username = 'Frederik Van Eecke'      # Vervang dit als je via een andere user queries stuurt
hostname = 'localhost'# Als je een databank lokaal draait, is dit localhost.
db = 'lahman2016'      # Naam van de gegevensbank op je XAMPP Mysql server

# We verbinden met de gegevensbank
c = verbind_met_GB(username, hostname, db)

# example query


def query_EX(connection, column_names, homeruns=20):
    # Bouw je query
    query = """
    select    t.name, t.yearID, t.HR
    from      Teams as t
    where     t.HR > {}
    order by  t.HR DESC;
    """.format(homeruns)  # TIP: Zo krijg je parameters in de string (samen met `{}` in de string)
    # Stap 2 & 3
    res = run_query(connection, query)  # Query uitvoeren
    df = res_to_df(res, column_names)  # Query in DataFrame brengen

    return df


# example query
# De voorbeeldquery heeft dezelfde kolomnamen als query 1, dus we gebruiken die
kolomnamen_voorbeeldquery = col_names['query_01']

# Functie uitvoeren, geeft resultaat van de query in een DataFrame
df = query_EX(c, kolomnamen_voorbeeldquery, homeruns=10)

# We inspecteren de eerste paar resultaten (voor alles te zien: laat .head() weg)


# query 01
def query_01(connection, column_names):
    # Bouw je query
    query = """
    select t.name, t.yearID, t.HR
    from teams as t
    order by t.HR DESC;
    """

    # Stap 2 & 3
    res = run_query(connection, query)  # Query uitvoeren
    df = res_to_df(res, column_names)  # Query in DataFrame brengen

    return df


kolomnamen_query01 = col_names['query_01']

df_query01 = query_01(c, kolomnamen_query01)
print(df_query01.head())


# query 02
def query_02(connection, column_names, datum_x = '1980-01-16'):
    # Bouw je query
    query = """
    select m.nameFirst, m.nameLast, m.birthDay, m.birthMonth, m.birthDay
    from master as m
    where m.debut > {}
    order by m.nameLast  ASC
    """.format(datum_x)
    # Stap 2 & 3
    res = run_query(connection, query)  # Query uitvoeren
    df = res_to_df(res, column_names)  # Query in DataFrame brengen

    return df


kolomnamen_query02 = col_names['query_02']
df_query02 = query_02(c, kolomnamen_query02, datum_x='1980-01-16')
print(df_query02.head())


# query 3

def query_03(connection, column_names):
    # Bouw je query
    query = """
        CREATE VIEW MNGR 
    AS 
    (SELECT mngr.playerID, mngr.teamID
    FROM managers as mngr
    WHERE mngr.plyrMgr ='N') 
    UNION 
    (SELECT mngr.playerID, mngr.teamID
    FROM managershalf as mngr
    );
    
    SELECT t.name, mstr.nameFirst, mstr.nameLast 
    FROM MNGR, master as mstr, team as t
    WHERE MNGR.

    
    select distinct t.name nameFirst nameLast 
    from (managers left outer join master on managers.playerID = master.playerID), team as t
    where plyrMgr = 'N' and teamID = t.teamID;
    """
    # Stap 2 & 3
    res = run_query(connection, query)  # Query uitvoeren
    df = res_to_df(res, column_names)  # Query in DataFrame brengen

    return df


kolomnamen_query03 = col_names['query_03']
df_query03 = query_03(c, kolomnamen_query03)
print(df_query03.head())
