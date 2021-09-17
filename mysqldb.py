from sqlalchemy import create_engine, text
import pandas as pd

class MySQLConnect:
    def __init__(self, host, user, passwd, db_name, port=3306):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db_name = db_name
        self.port = port
        self.engine = None
        print(host, db_name, 'db connected')
        

    def db_connection(self):
        self.engine = create_engine(
            f'mysql+pymysql://{self.user}:{self.passwd}@{self.host}:{self.port}/{self.db_name}?charset=utf8'
        )
        self.conn = self.engine.connect()
        return self.engine, self.conn

    def db_query(self, query: str):
        try:
            self.db_connection()
            conn = self.engine.connect().execution_options(autocommit=True)
            conn.execute(text(query))

        except Exception as e:
            print('Error:', e)

        return pd.read_sql(query, conn)

    def db_upload(self, data, table_name):
        try:
            self.db_connection()
            data.to_sql(name=table_name,
                        con=self.engine,
                        if_exists='append',
                        index=False)
            print(f'Your data has been uploaded to {table_name} successfully.')

        except Exception as e:
            print('Error:', e)
```