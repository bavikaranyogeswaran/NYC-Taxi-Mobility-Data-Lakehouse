import os

# Superset specific config
ROW_LIMIT = 5000
SUPERSET_WORKERS = 2

# Secret key for Flask sessions
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "s0m3_s3cr3t_k3y_for_superset_12345")

# The SQLAlchemy connection string to the metadata database.
# This points Superset to Postgres instead of the default local SQLite file.
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SUPERSET_SQLALCHEMY_DATABASE_URI",
    "postgresql+psycopg2://superset:superset@postgres/superset"
)

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = False
# Add endpoints that need to be exempt from CSRF protection
WTF_CSRF_EXEMPT_LIST = []
# A CSRF token that expires in 1 year
WTF_CSRF_TIME_LIMIT = 60 * 60 * 24 * 365

# Enable the REST API
FAB_API_SWAGGER_UI = True
