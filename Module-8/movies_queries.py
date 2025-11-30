""" import statements """
import mysql.connector  # to connect
from mysql.connector import errorcode

import dotenv # to use .env file
from dotenv import dotenv_values

# using our .env file
secrets = dotenv_values(".env")

""" database config object """
config = {
    "user": secrets["USER"],
    "password": secrets["PASSWORD"],
    "host": secrets["HOST"],
    "database": secrets["DATABASE"],
    "raise_on_warnings": True  # not in .env file
}

try:
    """ try/catch block for handling potential MySQL database errors """

    db = mysql.connector.connect(**config) # connect to the movies database

    # create a cursor object to execute queries
    cursor = db.cursor()

    # output the connection status
    print("\n  Database user {} connected to MySQL on host {} with database {}\n".format(
        config["user"], config["host"], config["database"]
    ))

    # ------------------------------------------------------------------
    # 1) Select all fields for the studio table
    # ------------------------------------------------------------------
    print("  -- DISPLAYING Studio RECORDS --")

    cursor.execute("SELECT * FROM studio")
    studios = cursor.fetchall()

    for studio in studios:
        # assuming studio table = (studio_id, studio_name)
        print("  Studio ID:   {}".format(studio[0]))
        print("  Studio Name: {}\n".format(studio[1]))

    # ------------------------------------------------------------------
    # 2) Select all fields for the genre table
    # ------------------------------------------------------------------
    print("  -- DISPLAYING Genre RECORDS --")

    cursor.execute("SELECT * FROM genre")
    genres = cursor.fetchall()

    for genre in genres:
        # assuming genre table = (genre_id, genre_name)
        print("  Genre ID:   {}".format(genre[0]))
        print("  Genre Name: {}\n".format(genre[1]))

    # ------------------------------------------------------------------
    # 3) Movie names with run time less than two hours (120 minutes)
    # ------------------------------------------------------------------
    print("  -- DISPLAYING Short Film RECORDS (run time < 120 minutes) --")

    cursor.execute("""
        SELECT film_name, film_runtime 
        FROM film 
        WHERE film_runtime < 120
    """)
    short_films = cursor.fetchall()

    for film in short_films:
        print("  Film Name: {}".format(film[0]))
        print("  Runtime:   {} minutes\n".format(film[1]))

    # ------------------------------------------------------------------
    # 4) List of film names and directors grouped by director
    #    (we order by director so films appear grouped together)
    # ------------------------------------------------------------------
    print("  -- DISPLAYING Film RECORDS Grouped by Director --")

    cursor.execute("""
        SELECT film_director, film_name 
        FROM film 
        ORDER BY film_director, film_name
    """)
    films_by_director = cursor.fetchall()

    for film in films_by_director:
        print("  Director: {}".format(film[0]))
        print("  Film:     {}\n".format(film[1]))

    input("\n\n  Press any key to continue...")

except mysql.connector.Error as err:
    """ on error code """

    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("  The supplied username or password are invalid")

    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("  The specified database does not exist")

    else:
        print(err)

finally:
    """ close the connection to MySQL """
    try:
        db.close()
    except NameError:
        # db was never created because connection failed
        pass