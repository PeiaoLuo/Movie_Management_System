import click

from Movie_sys import app, db
from Movie_sys.models import User

# @app.cli.command()
# @click.option('--drop', is_flag=True, help='Create after drop.')
# def initdb(drop):
#     """Initialize the database."""
#     if drop:
#         db.drop_all()
#     db.create_all()
#     click.echo('Initialized database.')
    
# @app.cli.command()
# @click.option('--username', prompt=True, help='The username used to login.')
# @click.option('--password', prompt=True, hide_input=True, confirmation_prompt=True, help='The password used to login.')
# def admin(username, password):
#     """Create user."""

#     user = User.query.first()
#     if user is not None:
#         click.echo('Updating user...')
#         user.username = username
#         user.set_password(password)
#     else:
#         click.echo('Creating user...')
#         user = User(username=username, name='Admin')
#         user.set_password(password)
#         db.session.add(user)

#     db.session.commit()
#     click.echo('Done.')