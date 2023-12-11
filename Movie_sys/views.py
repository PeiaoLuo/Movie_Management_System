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
    sql_query_actor = text("""
    SELECT
        a.actor_name AS actor_name,
        a.country AS actor_country,
        a.gender,
        GROUP_CONCAT(DISTINCT CASE WHEN mar.relation_type = '导演' THEN m.movie_name END) AS directed_movies,
        GROUP_CONCAT(DISTINCT CASE WHEN mar.relation_type != '导演' THEN m.movie_name END) AS acted_movies
    FROM
        actor_info a
    LEFT JOIN
        movie_actor_relation mar ON a.actor_id = mar.actor_id
    LEFT JOIN
        movie_info m ON mar.movie_id = m.movie_id
    LEFT JOIN
        movie_box mb ON m.movie_id = mb.movie_id
    GROUP BY
        a.actor_name, a.country, a.gender;
    """)
    
    if request.method == "POST":
        if 'submit_movie' in request.form:
            # Execute the query and fetch the result
            result_proxy = connection.execute(sql_query)
            result = result_proxy.fetchall()
            columns = result_proxy.keys()
            connection.close()
            
            # Convert into dataframe
            result = [dict(zip(columns, row)) for row in result]
            info = pd.DataFrame(result).astype(str)
            
            form_data = {}
            for key, value in request.form.items():
                form_data[key] = value
                
            filtered_dict = {key: value for key, value in form_data.items() if value != ""}
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
            final_info = info
            return render_template('search_result.html', table=final_info.to_html(index=False))
        else:
            #Excute the query
            result_proxy_actor = connection.execute(sql_query_actor)
            result_actor = result_proxy_actor.fetchall()
            columns_actor = result_proxy_actor.keys()

            connection.close()
            #Convert into dataframe
            result_actor = [dict(zip(columns_actor, row)) for row in result_actor]
            info_actor = pd.DataFrame(result_actor).astype(str)
            
            form_data = {}
            for key, value in request.form.items():
                form_data[key] = value
                
            filtered_dict = {key: value for key, value in form_data.items() if value != ""}
            filtered_dict.popitem()
            if filtered_dict == {}:
                flash('Please at least input one information.')
                return redirect(url_for('search'))

            for key, value in filtered_dict.items():
                info_actor = info_actor[info_actor[key]==value]

            final_info = info_actor
            return render_template('actor_result.html', table=final_info.to_html(index=False))
        
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
        
        #if date is "" , date=none
        if form_data['year'] != "":
            year = int(form_data['year'])
        else:
            year = None
            
        if date_string:
            parsed_date = datetime.strptime(date_string, '%d/%m/%Y').date()
        else:
            parsed_date = None
            
        movie_info = Movie_info(movie_id=new_movie_id,
                                movie_name=form_data['movie_name'],
                                release_date=parsed_date,
                                country=form_data['movie_country'],
                                type=form_data['type'],
                                year=year)

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
        if form_data['box'] != "":
            box = float(form_data['box'])
        else:
            box = None
            
        movie_box = Movie_box(movie_id=new_movie_id,
                              box=box)
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
        if len(actor_ls) > 0:
            for i in range(len(actor_ls)):
                #check if the actor existed
                actor = Actor_info.query.filter_by(actor_name=actor_ls[i]).first()
                if actor is not None:
                    relation_actor_id = actor.actor_id
                else:
                    #if not existed add actor_info
                    actor_info = Actor_info(actor_id=str(last_actor_id+1),
                                            actor_name=actor_ls[i],
                                            gender=sex_ls[i],
                                            country=country_ls[i])
                    db.session.add(actor_info)
                    last_actor_id += 1
                    relation_actor_id = last_actor_id
                #add relation
                movie_actor_relation = Movie_actor_relation(id=str(last_relation_id+1),
                                                            movie_id=new_movie_id,
                                                            actor_id=relation_actor_id,
                                                            relation_type="主演")
                last_relation_id += 1
                db.session.add(movie_actor_relation)                   
        #add director_info and relation 
        if len(dir_ls) > 0:
            for i in range(len(dir_ls)):
                #check if director existed
                director = Actor_info.query.filter_by(actor_name=dir_ls[i]).first()
                if director is not None:
                    relation_director_id = director.actor_id
                else:
                    #if not existed, add actor_info
                    director_info = Actor_info(actor_id=str(last_actor_id+1),
                                            actor_name=dir_ls[i],
                                            gender=dir_sex_ls[i],
                                            country=dir_country_ls[i])
                    db.session.add(director_info)
                    last_actor_id += 1
                    relation_director_id = last_actor_id
                #add relation
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

@app.route('/box_analysis',methods=['GET','POST'])
def analysis():
    sql_query_all = text("""
    SELECT
        movie_info.movie_id,
        movie_info.movie_name,
        DATE_FORMAT(movie_info.release_date, '%Y-%m-%d') AS release_date,
        movie_info.country AS movie_country,
        movie_info.type,
        movie_info.year,
        movie_box.box,
        actor_info.actor_name,
        actor_info.country as actor_country,
        actor_info.gender as sex,
        movie_actor_relation.relation_type
    FROM
        actor_info
    JOIN
        movie_actor_relation ON actor_info.actor_id = movie_actor_relation.actor_id
    JOIN
        movie_info ON movie_info.movie_id = movie_actor_relation.movie_id
    JOIN
        movie_box ON movie_info.movie_id = movie_box.movie_id
	order by movie_id
    """)
    connection = db.engine.connect()
    result_box = connection.execute(sql_query_all)
    box = result_box.fetchall()
    columns_box = result_box.keys()
    connection.close()
    #Convert into dataframe
    box = [dict(zip(columns_box, row)) for row in box]
    info_box = pd.DataFrame(box).astype(str)
    info_box['box'] = info_box['box'].astype('float64')
    info_box['box'] = info_box['box'].round(2)
    print(info_box)
    if request.method == "POST":
        # actor box analysis
        if 'submit_actor' in request.form:
            form_data = {}
            for key, value in request.form.items():
                form_data[key] = value
                
            filtered_dict = {key: value for key, value in form_data.items() if value != ""}
            filtered_dict.popitem()
            if filtered_dict == {}:
                flash('Please at least input one information.')
                return redirect(url_for('analysis'))
            
            new_info = info_box.copy()
            # print(new_info)
            # get avg and var group by actor
            avg_total = new_info[['actor_name','box']].groupby('actor_name').mean()['box'].mean()
            avg_var_total = new_info[['actor_name','box']].groupby('actor_name').var()['box'].mean()
            
            avg_actor = new_info[new_info['relation_type']=="主演"][['actor_name','box']].groupby('actor_name').mean()['box'].mean()
            avg_var_actor = new_info[new_info['relation_type']=="主演"][['actor_name','box']].groupby('actor_name').var()['box'].mean()
            
            avg_director = new_info[new_info['relation_type']=="导演"][['actor_name','box']].groupby('actor_name').mean()['box'].mean()
            avg_var_director = new_info[new_info['relation_type']=="导演"][['actor_name','box']].groupby('actor_name').var()['box'].mean()
            
            # get table and flag of box(higher or lower than avg_total)
            acted_movies = new_info[(new_info['relation_type']=="主演") & (new_info['actor_name']==filtered_dict['actor_name'])].copy()
            if acted_movies.empty:
                acted_movies = None
            else:
                acted_movies = acted_movies.iloc[:,1:7].sort_values(by='box',ascending=False)
                acted_mean = float(acted_movies['box'].mean())
                acted_var = float(acted_movies['box'].var())
                acted_dic = {"avg":round(acted_mean,2), "var":round(acted_var,2)}
                if acted_mean > avg_actor:
                    acted_avg_higher = 1
                else:
                    acted_avg_higher = 0
                if acted_var > avg_var_actor:
                    acted_var_higher = 1
                else:
                    acted_var_higher = 0
                acted_movies = acted_movies.to_html(index=False)
    
            directed_movies = new_info[(new_info['relation_type']=="导演") & (new_info['actor_name']==filtered_dict['actor_name'])].copy()
            if directed_movies.empty:
                if acted_movies is None:
                    flash('No info of this actor.')
                    return redirect(url_for('analysis'))
                else:
                    directed_movies = None
            else:
                directed_movies = directed_movies.iloc[:,1:7].sort_values(by='box',ascending=False)
                directed_mean = float(directed_movies['box'].mean())
                directed_var = float(directed_movies['box'].var())
                directed_dic = {"avg":round(directed_mean,2), "var":round(directed_var,2)}
                if directed_mean > avg_director:
                    directed_avg_higher = 1
                else:
                    directed_avg_higher = 0
                if directed_var > avg_var_director:
                    directed_var_higher = 1
                else:
                    directed_var_higher = 0
                directed_movies = directed_movies.to_html(index=False)
            
            its_movies = new_info[new_info['actor_name']==filtered_dict['actor_name']].copy()
            its_movies = its_movies.iloc[:,1:7].sort_values(by='box',ascending=False)
            total_mean = float(its_movies['box'].mean())
            total_var = float(its_movies['box'].var())
            total_dic = {"avg":round(total_mean,2), "var":round(total_var,2)}
            if total_mean > avg_total:
                total_avg_higher = 1
            else:
                total_avg_higher = 0
            if total_var > avg_var_total:
                total_var_higher = 1
            else:
                total_var_higher = 0
                    
            return render_template('box_result_actor.html',actor_name=filtered_dict['actor_name'],directed_movies=directed_movies,acted_movies=acted_movies,
                                   high_avg_directed=directed_avg_higher,high_var_directed=directed_var_higher,high_avg_acted=acted_avg_higher,high_var_acted=acted_var_higher,
                                   high_avg_total=total_avg_higher, high_var_total=total_var_higher,
                                    acted_dict=acted_dic, directed_dict=directed_dic, total_dict=total_dic)
        # type box analysis
        else:
            form_data = {}
            for key, value in request.form.items():
                form_data[key] = value
                
            filtered_dict = {key: value for key, value in form_data.items() if value != ""}
            filtered_dict.popitem()
            if filtered_dict == {}:
                flash('Please at least input one information.')
                return redirect(url_for('analysis'))
            
            new_info = info_box.iloc[:,1:7].copy().drop_duplicates()
            # print(new_info)
            # get avg and var group by type
            
            avg_type = new_info[['type','box']].groupby('type').mean()['box'].mean()
            avg_var_type = new_info[['type','box']].groupby('type').var()['box'].mean()
            
            # get table and flag of box(higher or lower than avg_total)
            type_movies = new_info[new_info['type']==filtered_dict['type']].copy()
            if type_movies.empty:
                flash('No info of this type.')
                return redirect(url_for('analysis'))
            else:
                type_movies = type_movies.sort_values(by='box',ascending=False)
                type_mean = float(type_movies['box'].mean())
                type_var = float(type_movies['box'].var())
                type_dic = {"avg":round(type_mean,2), "var":round(type_var,2)}
                if type_mean > avg_type:
                    type_avg_higher = 1
                else:
                    type_avg_higher = 0
                if type_var > avg_var_type:
                    type_var_higher = 1
                else:
                    type_var_higher = 0
                type_movies = type_movies.to_html(index=False)
    
            return render_template('box_result_type.html',type=filtered_dict['type'],type_dict=type_dic,type_movies=type_movies,
                                   high_avg_type=type_avg_higher, high_var_type=type_var_higher)
    
    return render_template('box.html')