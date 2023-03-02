import os
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
#from config import config_dict

def create_app(config_name):
    # Initialize app with configuration
    app = Flask(__name__)
    config_dict = {}
    app.config.from_object(config_dict[config_name])
    app.config['SECREY_KEY'] = "Ingredion"
    config_dict[config_name].init_app(app)
    print(config_dict[config_name])

    # Initialize extentions

    # Register blueprints
    from .views import performance_views
    app.register_blueprint(performance_views.bp)
