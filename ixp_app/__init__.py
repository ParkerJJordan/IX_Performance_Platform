import os
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
#from config import config_dict

#db = SQLAlchemy()

def create_app(config_name):
    # Initialize app with configuration
    app = Flask(__name__)

    config_dict = {
    'development': DevelopmentConfig,
    'testing': TestingConfig,
    'production': ProductionConfig,
    'default': ProductionConfig}

    app.config.from_object(config_dict[config_name])
    app.config['SECRET_KEY'] = "Ingredion"
    config_dict[config_name].init_app(app)
    print(config_dict[config_name])

    # Initialize extentions
    #db.init_app(app)

    # Register blueprints
    from .views import performance_views
    app.register_blueprint(performance_views.bp)

    return app

class Config:
    SECREY_KEY = "Ingredion"
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    @staticmethod
    def init_app(app):
        pass

class ProductionConfig(Config):
    SQLAlchemy_DATABASE_URI = ''
    ASPEN_SERVER = 'ARGPCS19'

class DevelopmentConfig(Config):
    DEBUG = True
    SQLAlchemy_DATABASE_URI = ''
    ASPEN_SERVER = 'ARGPCS19'

class TestingConfig(Config):
    TESTING = True
    SQLAlchemy_DATABASE_URI = ''
    ASPEN_SERVER = 'ARGPCS19'