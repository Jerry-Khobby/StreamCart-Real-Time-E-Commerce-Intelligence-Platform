FROM apache/superset:4.1.4

USER root
# The base image ships without a Postgres driver.
RUN pip install --no-cache-dir psycopg2-binary==2.9.9
COPY superset_config.py /app/pythonpath/superset_config.py
COPY build_bundle.py init.sh /app/streamcart/
RUN sed -i 's/\r$//' /app/streamcart/init.sh && chmod +x /app/streamcart/init.sh
USER superset
