from . import db


class ResinReplacements(db.Model):
    __tablename__ = 'resin_replacements'
    index = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.Date)
    unit = db.Column(db.String)
    unitname = db.Column(db.String)
    columntype = db.Column(db.String)

    def __repr__(self):
        return f'<Resin Replacenents {self.index}>'
