from flask import render_template, url_for, redirect, flash, request
from flask_login import login_user, login_required, logout_user, current_user
from sqlalchemy import text
from Movie_sys import app, db
from Movie_sys.models import User, Movie_info, Actor_info, Movie_box, Movie_actor_relation

@app.route('/', methods=['GET','POST'])
def index():
    if request.method == "POST":
        form_type = request.form.get('form_type')
        if form_type == "Search":
            pass
        elif form_type == "Add":
            pass
        else:
            flash('Unknown form.')
    
    connection = db.engine.connect()

    # sql language beneath
    sql_query = text("""
    SELECT
        movie_info.movie_id,
        movie_info.movie_name,
        DATE_FORMAT(movie_info.release_date, '%Y-%m-%d') AS release_date,
        movie_info.country AS movie_info_country,
        movie_info.type,
        movie_info.year,
        movie_box.box,
        GROUP_CONCAT(actor_info.actor_name SEPARATOR ', ') AS actor_names
    FROM
        actor_info
    JOIN
        movie_actor_relation ON actor_info.actor_id = movie_actor_relation.actor_id
    JOIN
        movie_info ON movie_info.movie_id = movie_actor_relation.movie_id
    JOIN
        movie_box ON movie_info.movie_id = movie_box.movie_id
    GROUP BY
        movie_info.movie_id, movie_info.movie_name, movie_info.release_date, 
        movie_info.country, movie_info.type, movie_info.year, movie_box.box, 
        movie_actor_relation.relation_type
	having relation_type='主演'
    """)

    # Execute the query and fetch the result
    result_proxy = connection.execute(sql_query)
    result = result_proxy.fetchall()
    columns = result_proxy.keys()

    # Close the connection
    connection.close()

    # Convert each RowProxy to a dictionary
    result = [dict(zip(columns, row)) for row in result]
    return render_template('index.html', movies=result)