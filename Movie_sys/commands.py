import click
from . import sql2py
from datetime import datetime
from sqlalchemy import text
from Movie_sys import app, db
from Movie_sys.models import User, Movie_info, Movie_box, Movie_actor_relation, Actor_info

@app.cli.command()
def regendb():

    click.echo('Clearing database.')
    db.drop_all()
    click.echo('Database cleared.')
    click.echo('Initializing database...')
    db.create_all()
    click.echo('Database initialized.')
    
    click.echo('Data generating...')
    dict_ls = []
    sql2py.sql2dict(dict_ls)
    for item in dict_ls:
        for k ,v in item.items():
            key=k
            values=v
        if key == 'movie_info':
            if len(values) != 6:
                print(f'Missmatch table and values: {key}:{values}')
                exit(-1)
            movie_info = Movie_info(movie_id=values[0],
                                movie_name=values[1],
                                release_date=datetime.strptime(values[2], '%Y/%m/%d').date(),
                                country=values[3],
                                type=values[4],
                                year=int(values[5]))
            db.session.add(movie_info)
        elif key == 'actor_info':
            if len(values) != 4:
                print(f'Missmatch table and values: {key}:{values}')
                exit(-1)
            actor_info = Actor_info(actor_id=values[0],
                                        actor_name=values[1],
                                        gender=values[2],
                                        country=values[3])
            db.session.add(actor_info)
        elif key == 'movie_actor_relation':
            if len(values) != 4:
                print(f'Missmatch table and values: {key}:{values}')
                exit(-1)
            movie_actor_relation = Movie_actor_relation(id=values[0],
                                                        movie_id=values[1],
                                                        actor_id=values[2],
                                                        relation_type=values[3])
            db.session.add(movie_actor_relation)
        elif key == 'movie_box':
            if len(values) != 2:
                print(f'Missmatch table and values: {key}:{values}')
                exit(-1)
            movie_box = Movie_box(movie_id=values[0],
                              box=float(values[1]))
            db.session.add(movie_box)
        else:
            print(f'No such table: {key}')
            exit(-1)
    db.session.commit()
    click.echo('Data generated.')
    