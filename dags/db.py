from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from settings import db_user, db_password, db_server, port, db, schema

engine = create_engine(
    'postgresql+psycopg2://{user}:{password}@{server}:{port}/{db}'.format(
        user=db_user,
        password=db_password,
        server=db_server,
        port=port,
        db=db),
    connect_args={'options': '-csearch_path={schema}'.format(
        schema=schema)
    })
db_session = scoped_session(sessionmaker(bind=engine))

Base = declarative_base()
Base.query = db_session.query_property()
