import datetime
import json
from typing import List,Dict

import mysql.connector

_DB_PARAM_PATH = "/home/im/mypy/ping/db/db_param.json"

class MyDb:
    """
    Actions with MySQL database.
    Database is connected once (result stored in class-level attributes).
    Each instance is just a cursor. Many cursor can be opened/operated/closed independently.
    """
    db_param : Dict[str,str] = None # params from file
    mydb = None # mysql.connector.connect when connected

    @classmethod
    def _init_db(cls, param_path):
        """ set MyDb.db_param based on the json file """
        if cls.mydb is not None: # already connected
            return

        try:
            f = open(param_path,"r")
        except FileNotFoundError:
            print(f"File {param_path} doesn't exists. Can't connect to database.")
            raise

        try:
            cls.db_param = json.load(f)
        except json.JSONDecodeError:
            print(f"Error while decoding json file {param_path} with db parameters")
            raise

        cls.mydb = mysql.connector.connect(
            host=cls.db_param['host'], user=cls.db_param['user'], passwd=cls.db_param['passwd'],
            database=cls.db_param['database'], autocommit=True, charset='latin1' # ,use_unicode=True, buffered=True
            )
        if cls.mydb is None:
            raise RuntimeError (f"Failed to connnect to database: {cls.db_info()}")
        else:
            print(f"Connected to databse: {cls.db_info()}")

    @classmethod
    def db_info(cls):
        return f"host={cls.db_param['host']} user={cls.db_param['user']} database={cls.db_param['database']}"

    def __init__(self, param_path = _DB_PARAM_PATH):
        """
        open cursor; connect to database if necessary
        :param param_path: path to json file with db parameters
        """
        self._init_db(param_path)
        self.curs = self.mydb.cursor()

    def close(self):
        """ close cursor """
        if self.curs is not None:
            self.curs.close()

    def exec(self, statement: str, args: List[str] = None, show=True) -> List[str]:
        """
        execute SQL statement with args; show result if show==True
        :return: cursor converted to list of strings (one record - one string)
        """
        if show:
            print(f"Exec:{statement}")
        try:
            self.curs.execute(statement, args)
        except  mysql.connector.Error as err:
            print(f"Something went wrong while exec {statement}: {format(err)}")
            raise
        result_str = [s for s in self.curs]
        if show and result_str:
            print("     results:")
            for ind, ln in enumerate(result_str):
                print(f"row {ind}: {ln}")
        return result_str

class AuxSQL:
    """
    auxiliary functions for MySQL
    """
    @staticmethod
    def datetime_2_sql(dt: datetime.datetime) -> str:
        """ python datetime.datetime --> MySQL datetime """
        return dt.strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def sql_2_datetime(s: str) -> datetime.datetime:
        """ sql datetime --> python datetime.datetime """
        return datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')

    @classmethod
    def now_timestamp(cls) -> str:
        """ python datetime.datetime.now() --> sql datetime """
        return cls.datetime_2_sql(datetime.datetime.now())

    @staticmethod
    def bool_2_sql(val: bool) -> str:
        """ python bool -> sql bool """
        return str(val).upper()


if __name__ == '__main__':
    db = MyDb()
    db.exec('drop table if exists test_mydb')
    db.exec('create table test_mydb (name varchar(20), val float)')
    db.exec('describe test_mydb')
    db.exec('insert into test_mydb values ("aa",1), ("bb", 2)')
    res = db.exec('select * from test_mydb')
    print("received from db.exec:")
    print("\n".join([f"{ind}: name={line[0]} val={line[1]}" for ind, line in enumerate(res)]))
