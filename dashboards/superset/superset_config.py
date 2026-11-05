import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://{u}:{p}@postgres:5432/superset".format(
    u=os.environ["POSTGRES_USER"], p=os.environ["POSTGRES_PASSWORD"]
)
ROW_LIMIT = 10000
