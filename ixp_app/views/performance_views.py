from flask import Blueprint, render_template, redirect, url_for, flash
from flask_wtf import FlaskForm
from wtforms import IntegerField, SubmitField, StringField, SelectField, RadioField
import pandas as pd
from ixp_app import db
#from ixp_app.models import 
from wtforms.validators import DataRequired, InputRequired
from ixp_app.services.common import render_dataframe
from ixp_app.services.ixperformance import IXPerformance

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
    
    select = SelectField(choices=pairselect, default='41IXA')
    duration = IntegerField('Timespan [Days]', default=365, validators=[InputRequired()])

@bp.route('/ixperformance', methods=['GET', 'POST'])
def ixperfom(pair_select):

    ixperfom_table = None
    pairname = pair_select

    func = IXPerformance(pairname=pairname, timespan=timespan)
    ixperfom_table = func.performance

    ixperfom_table = render_dataframe(ixperfom_table, True)
    ix_form = IXPerformanceForm(prefix='ixperform')

    if ix_form.validate_on_submit() and ix_form.select.data:
        pairname = str(ix_form.select.data)
        timespan = int(ix_form.duration.data)
        return redirect(url_for('.ixperform',
                        pair_select=pairname,
                        duration=timespan))

    return render_template('ixperformance/performance_results.html', 
                           ix_form=ix_form,
                           cycle_table=ixperfom_table)

@bp.cli.command('init')
def load_resin_replacements_db():
    resin_replacements_path = 'ixp_app/data/raw/resin.xlsx'

    resin_replacments = pd.read_excel(resin_replacements_path, sheet_name=0, parse_dates=['Date'])
    resin_replacments.to_sql('resin_replacements',
                             db.engine,
                             if_exists='replace',
                             index=True,
                             index_label='index')
    
    print('Successfully loaded resin replacment data into the database')

    return True
