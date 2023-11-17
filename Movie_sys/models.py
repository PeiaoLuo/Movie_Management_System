from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
from Movie_sys import db

class User(db.Model, UserMixin):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(20))
    username = db.Column(db.String(20))
    password_hash = db.Column(db.String(256))
    
    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def validate_password(self, password):
        return check_password_hash(self.password_hash, password)
    
class Actor_info(db.Model):
    actor_id = db.Column(db.String(10), primary_key=True)
    actor_name = db.Column(db.String(20))
    gender = db.Column(db.String(2))
    country = db.Column(db.String(20))

class Movie_info(db.Model):
    movie_id = db.Column(db.String(10), primary_key=True)
    movie_name = db.Column(db.String(20))
    release_date = db.Column(db.Date)
    country = db.Column(db.String(20))
    type = db.Column(db.String(10))
    year = db.Column(db.Integer)

class Movie_box(db.Model):
    movie_id = db.Column(db.String(10), primary_key=True)
    box = db.Column(db.Float)
    
class Movie_actor_relation(db.Model):
    id = db.Column(db.String(10), primary_key=True)
    actor_id = db.Column(db.String(10))
    movie_id = db.Column(db.String(10))
    relation_type = db.Column(db.String(20))
    