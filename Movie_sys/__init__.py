from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager

app = Flask(__name__)

HOSTNAME = "localhost"
PORT = 3306
USERNAME = "root"
PASSWD = "Lpa112211"
DATABASE = "moviedb"

app.config['SECRET_KEY'] = 'dev'
app.config['SQLALCHEMY_DATABASE_URI'] = app.config['SQLALCHEMY_DATABASE_URI']=f"mysql+pymysql://{USERNAME}:{PASSWD}@{HOSTNAME}/{DATABASE}?charset=utf8mb4"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db=SQLAlchemy(app)
# login_manager = LoginManager(app)

# @login_manager.user_loader
# def load_user(user_id):
#     from Movie_sys.models import User
#     user = db.session.query(User).get(int(user_id))
#     return user

# login_manager.login_view = 'login'

# @app.context_processor
# def inject_user():
#     from Movie_sys.models import User
#     user = User.query.first()
#     return dict(user=user)

from Movie_sys import views, errors, commands