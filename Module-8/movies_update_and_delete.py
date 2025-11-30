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


def show_films(cursor, title):
    # method to execute an inner join on all tables,
    # iterate over the dataset and output the results to the terminal window.

    # inner join query
    cursor.execute("""
        SELECT film_name AS Name,
               film_director AS Director,
               genre_name AS 'Genre Name ID',
               studio_name AS 'Studio Name'
        FROM film
        INNER JOIN genre ON film.genre_id = genre.genre_id
        INNER JOIN studio ON film.studio_id = studio.studio_id
    """)

    # get the results from the cursor object
    films = cursor.fetchall()

    print("\n -- {} --".format(title))

    # iterate over the film data set and display the results
    for film in films:
        print("Film Name: {}\nDirector: {}\nGenre Name ID: {}\nStudio Name: {}\n".format(
            film[0], film[1], film[2], film[3]
        ))


try:
    """ try/catch block for handling potential MySQL database errors """

    db = mysql.connector.connect(**config) # connect to the movies database

    # create a cursor object to execute queries
    cursor = db.cursor()

    # output the connection status
    print("\n  Database user {} connected to MySQL on host {} with database {}\n".format(
        config["user"], config["host"], config["database"]
    ))

    # Using the example code, call the show_films function to display the selected fields and the associated Label.
    show_films(cursor, "DISPLAYING FILMS")

    # Insert a new record into the film table using a film of your choice (The Martian).
    insert_query = """
        INSERT INTO film (film_name, film_releaseDate, film_runtime,
                          film_director, studio_id, genre_id)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    new_film = ("The Martian", "2015", 144, "Ridley Scott", 1, 2)
    cursor.execute(insert_query, new_film)
    db.commit()

    # Using the example code, call the show_films function to display the selected fields and the associated Label.
    show_films(cursor, "DISPLAYING FILMS AFTER INSERT")

    # Using the example code, update the film Alien to being a Horror film.
    update_query = """
        UPDATE film
        SET genre_id = 1
        WHERE film_name = 'Alien'
    """
    cursor.execute(update_query)
    db.commit()

    # Using the example code, call the show_films function to display the selected fields and the associated Label.
    show_films(cursor, "DISPLAYING FILMS AFTER UPDATE- Changed Alien to Horror")

    # Using the example code, delete the movie Gladiator.
    delete_query = """
        DELETE FROM film
        WHERE film_name = 'Gladiator'
    """
    cursor.execute(delete_query)
    db.commit()

    # Using the example code, call the show_films function to display the selected fields and the associated Label.
    show_films(cursor, "DISPLAYING FILMS AFTER DELETE")

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