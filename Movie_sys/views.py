from flask import render_template, url_for, redirect, flash, request
from flask_login import login_user, login_required, logout_user, current_user
from sqlalchemy import text, cast, Integer
from Movie_sys import app, db
from Movie_sys.models import User, Movie_info, Actor_info, Movie_box, Movie_actor_relation
import pandas as pd
from datetime import datetime
import re

@app.route('/')
def index():    
    connection = db.engine.connect()

    # sql language beneath
    sql_query = text("""
    SELECT
        movie_info.movie_id,
        movie_info.movie_name,
        DATE_FORMAT(movie_info.release_date, '%Y-%m-%d') AS release_date,
        movie_info.country AS movie_country,
        movie_info.type,
        movie_info.year,
        movie_box.box,
        GROUP_CONCAT(CASE WHEN movie_actor_relation.relation_type = '导演' THEN actor_info.actor_name END SEPARATOR ', ') AS director,
        GROUP_CONCAT(CASE WHEN movie_actor_relation.relation_type = '主演' THEN actor_info.actor_name END SEPARATOR ', ') AS main_actor
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
        movie_info.country, movie_info.type, movie_info.year, movie_box.box;
    """)

    # Execute the query and fetch the result
    result_proxy = connection.execute(sql_query)
    result = result_proxy.fetchall()
    columns = result_proxy.keys()

    connection.close()

    # Convert each RowProxy to a dictionary
    result = [dict(zip(columns, row)) for row in result]
            
    return render_template('index.html', movies=result)

@app.route('/search', methods=["GET","POST"])
def search():
    connection = db.engine.connect()

    # sql language beneath
    sql_query = text("""
    SELECT
        movie_info.movie_name,
        movie_info.country AS movie_country,
        movie_info.type,
        movie_info.year,
        movie_box.box,
        GROUP_CONCAT(CASE WHEN movie_actor_relation.relation_type = '导演' THEN actor_info.actor_name END SEPARATOR ', ') AS director,
        GROUP_CONCAT(CASE WHEN movie_actor_relation.relation_type = '主演' THEN actor_info.actor_name END SEPARATOR ', ') AS main_actor
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
        movie_info.country, movie_info.type, movie_info.year, movie_box.box;
    """)

    # Execute the query and fetch the result
    result_proxy = connection.execute(sql_query)
    result = result_proxy.fetchall()
    columns = result_proxy.keys()

    # Close the connection
    connection.close()

    # Convert each RowProxy to a dictionary
    result = [dict(zip(columns, row)) for row in result]
    info = pd.DataFrame(result).astype(str)
    
    if request.method == "POST":
        form_data = {}
        for key, value in request.form.items():
            form_data[key] = value
            
        filtered_dict = {key: value for key, value in form_data.items() if value is not ""}
        filtered_dict.popitem()
        if filtered_dict == {}:
            flash('Please at least input one information.')
            return redirect(url_for('search'))

        # Initialize a boolean mask
        mask = pd.Series([True] * len(info), index=info.index)

        # Apply filters based on the dictionary
        for column, value in filtered_dict.items():
            mask &= info[column].str.contains(value, case=False)

        # Use the boolean mask to filter the DataFrame
        info = info[mask]

        return render_template('search_result.html', table=info.to_html(index=False))
    return render_template('search.html')

@app.route('/add',methods=["GET","POST"])
def add():
    if request.method == "POST":
        form_data = {}
        for key, value in request.form.items():
            form_data[key] = value
        form_data.popitem()
        #---------------------create movie_info row--------------------------
        if Movie_info.query.filter_by(movie_name=form_data['movie_name']).first() is not None:
            flash('Movie existed!')
            return redirect(url_for('add'))
        last_movie = (
            Movie_info.query
            .order_by(cast(Movie_info.movie_id, Integer).desc())
            .first()
        )
        new_movie_id = str(int(last_movie.movie_id)+1)
        date_string = form_data['release_date']
        parsed_date = datetime.strptime(date_string, '%d/%m/%Y').date()
        
        movie_info = Movie_info(movie_id=new_movie_id,
                                movie_name=form_data['movie_name'],
                                release_date=parsed_date,
                                country=form_data['movie_country'],
                                type=form_data['type'],
                                year=int(form_data['year']))

        #---------------------create movie_info row--------------------------
        
        #---------------------get actor_info--------------------------
        last_actor = (
            Actor_info.query
            .order_by(cast(Actor_info.actor_id, Integer).desc())
            .first()
        )
        last_actor_id = int(last_actor.actor_id)
        actor_ls = re.split(r',|，', form_data['actor_name'])
        country_ls = re.split(r',|，', form_data['actor_country'])
        sex_ls = re.split(r',|，', form_data['actor_sex'])
        if not len(actor_ls) == len(country_ls) == len(sex_ls):
            flash('Actor information mismatch, please check actor\'s name, country, sex input.')
            return redirect(url_for('add'))
        #---------------------get actor_info--------------------------
        
        #---------------------get director_info--------------------------
        dir_ls = re.split(r',|，', form_data['dir_name'])
        dir_country_ls = re.split(r',|，', form_data['dir_country'])
        dir_sex_ls = re.split(r',|，', form_data['dir_sex'])
        if not len(dir_ls) == len(dir_country_ls) == len(dir_sex_ls):
            flash('Director information mismatch, please check director\'s name, country, sex input.')
            return redirect(url_for('add'))
        #---------------------get director_info--------------------------
        
        #---------------------create movie_box row--------------------------
        movie_box = Movie_box(movie_id=new_movie_id,
                              box=float(form_data['box']))
        #---------------------create movie_box row--------------------------
        
        #---------------------get relationship--------------------------
        last_relation = (
            Movie_actor_relation.query
            .order_by(cast(Movie_actor_relation.id, Integer).desc())
            .first()
        )
        last_relation_id = int(last_relation.id)
        #---------------------get relationship--------------------------
        
        #add movie_info
        db.session.add(movie_info)
        db.session.add(movie_box)
        #add actor_info and relation
        for i in range(len(actor_ls)):
            actor = Actor_info.query.filter_by(actor_name=actor_ls[i]).first()
            if actor is not None:
                relation_actor_id = actor.actor_id
            else:
                actor_info = Actor_info(actor_id=str(last_actor_id+1),
                                        actor_name=actor_ls[i],
                                        gender=sex_ls[i],
                                        country=country_ls[i])
                db.session.add(actor_info)
                last_actor_id += 1
                relation_actor_id = last_actor_id
            movie_actor_relation = Movie_actor_relation(id=str(last_relation_id+1),
                                                        movie_id=new_movie_id,
                                                        actor_id=relation_actor_id,
                                                        relation_type="主演")
            last_relation_id += 1
            db.session.add(movie_actor_relation)                   
        #add director_info and relation 
        for i in range(len(dir_ls)):
            director = Actor_info.query.filter_by(actor_name=dir_ls[i]).first()
            if director is not None:
                relation_director_id = director.actor_id
            else:
                director_info = Actor_info(actor_id=str(last_actor_id+1),
                                        actor_name=dir_ls[i],
                                        gender=dir_sex_ls[i],
                                        country=dir_country_ls[i])
                db.session.add(director_info)
                last_actor_id += 1
                relation_director_id = last_actor_id
            movie_actor_relation = Movie_actor_relation(id=str(last_relation_id+1),
                                                        movie_id=new_movie_id,
                                                        actor_id=relation_director_id,
                                                        relation_type="导演")
            last_relation_id += 1
            db.session.add(movie_actor_relation)
        # commit change
        db.session.commit()
        flash('Movie added.')
        redirect(url_for('add'))

    return render_template('add.html')

@app.route('/delete',methods=["POST"])
def delete():
    movie_name = request.form['movie_name']
    movie = Movie_info.query.filter_by(movie_name=movie_name).first()
    if movie:
        movie_id = movie.movie_id
    else:
        flash('No such movie in the system.')
        return redirect(url_for('index'))

    movie_box = Movie_box.query.filter_by(movie_id=movie_id).first()
    #--------------get the rows of relation_info and actor_id to remove---------------------
    #get the actor_id that need to be removed
    all_actors = (
        Movie_actor_relation.query
        .with_entities(Movie_actor_relation.actor_id)
        .all()
    )
    all_actor_ids = [actor.actor_id for actor in all_actors]
    unique_actor_ids = [actor_id for actor_id in set(all_actor_ids) if all_actor_ids.count(actor_id) == 1]
    
    actors_in_movie = (
        Movie_actor_relation.query
        .filter_by(movie_id=movie_id)
        .all()
    )
    all_actor_ids_in_movie = set([actor.actor_id for actor in actors_in_movie])
    actor_ids_to_remove = [id for id in all_actor_ids_in_movie if id in unique_actor_ids]
    #--------------get the rows of relation_info and actor_id to remove---------------------
    
    db.session.delete(movie)
    db.session.delete(movie_box)
    for relation in actors_in_movie:
        db.session.delete(relation)
        
    #--------------delete actor_info rows--------------------
    if actor_ids_to_remove != []:
        for actor_id in actor_ids_to_remove:
            actor = Actor_info.query.filter_by(actor_id=actor_id).first()
            db.session.delete(actor)
    #--------------delete actor_info rows--------------------
    
    db.session.commit()
    flash('Movie deleted')
    return redirect(url_for('index'))