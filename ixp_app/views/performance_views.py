from flask import Blueprint, render_template, redirect, url_for, flash
from flask_wtf import FlaskForm
from wtforms import IntegerField, SubmitField, StringField, SelectField, RadioField
from ixp_app import db
from sqlalchemy.orm import sessionmaker
#from ixp_app.models import 
from wtforms.validators import DataRequired, InputRequired
from ixp_app.services.common import render_dataframe

bp = Blueprint('ixperform', __name__)

class IXPerformanceForm(FlaskForm):
    pairselect = ['41IXA', '41IXB', '41IXC',
                  '41IXD', '41IXE', '41IXF',
                  '41IXG', '41IXH', '41IXI',
                  '41IX1X', '41IX2X', '41IX3X',
                  '41IX1Y', '41IX2Y', '41IX3Y',
                  '41IX1Z', '41IX2Z', '41IX3Z',
                  '49IXA', '49IXB', '49IXC',
                  '49IXX', '49IXY', '49IXZ',
                  '42IXD', '42IXE', '42IXF', '42IXX']
    pairs_to_search = RadioField(choices=pairselect, default='41IXA')
    timespan = IntegerField('Days', default=365, validators=[InputRequired()])

@bp.route('/ixperformance', methods=['GET', 'POST'])
def ixperfom():
    return