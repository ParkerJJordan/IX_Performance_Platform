import os
from ixp_app import create_app, db
from ixp_app.models import ResinReplacements
from flask_migrate import Migrate

app = create_app('production')
migrate = Migrate(app, db)

if __name__ == "__main__":
    app.run=()


@app.shell_context_processor
def make_shell_context():
    return dict(db=db,
                ResinReplacements=ResinReplacements)
