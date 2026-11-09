import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
# Superset's own metadata, with a login that owns only that database (migration V10).
SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://{u}:{p}@postgres:5432/superset".format(
    u=os.environ["SUPERSET_DB_USER"], p=os.environ["SUPERSET_DB_PASSWORD"]
)
ROW_LIMIT = 10000
