import sqlalchemy as db

engine = db.create_engine('mysql://root:pass@127.0.0.1/DUMMY')
connection = engine.connect()

print(connection.execute('SELECT 1'))