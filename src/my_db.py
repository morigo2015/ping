import datetime
import json
from typing import List, Dict, Tuple, Any
from collections import namedtuple

import mysql.connector

_DB_PARAM_PATH = "/home/im/mypy/ping/config/db_param.json"

class MyDb:
    """
    Actions with MySQL database.
    Database is connected once (result stored in class-level attributes).
    Each instance is just a cursor. The cursors can be opened/operated/closed independently.
    """
    db_param: Dict[str, str] = None  # params from file
    mydb = None  # mysql.connector.connect when connected

    @classmethod
    def _init_db(cls, param_path=None):
        """ set MyDb.db_param based on the json file """
        if cls.mydb is not None:  # already connected
            return
        if param_path is None:
            param_path = _DB_PARAM_PATH

        try:
            f = open(param_path, "r")
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
            database=cls.db_param['database'], autocommit=True, charset='latin1'  # ,use_unicode=True, buffered=True
        )
        if cls.mydb is None:
            raise RuntimeError(f"Failed to connnect to database: {cls.db_info()}")
        else:
            print(f"Connected to database: {cls.db_info()}")

    @classmethod
    def db_info(cls):
        return f"host={cls.db_param['host']} user={cls.db_param['user']} database={cls.db_param['database']}"

    def __init__(self, param_path=None):
        """
        open cursor; connect to database if necessary
        :param param_path: path to json file with db parameters
        """
        self._init_db(param_path=param_path)
        self.curs = self.mydb.cursor()
        self.last_exec_result: List[Tuple[Any, ...]] = None

    def close(self):
        """ close cursor """
        if self.curs is not None:
            self.curs.close()

    def exec(self, statement: str, args: List[str] = None, show=True) -> List[Tuple[Any]]:
        """
        execute SQL statement with args; show result if show==True
        :return: cursor converted to list of tuples;
        """
        if show:
            print(f"Exec:{statement}")
        try:
            self.curs.execute(statement, args)
        except  mysql.connector.Error as err:
            print(f"Something went wrong while exec {statement}: {format(err)}")
            raise
        self.last_exec_result = [s for s in self.curs]
        if show and self.last_exec_result:
            print("     results:")
            for ind, ln in enumerate(self.last_exec_result):
                print(f"row {ind}: {ln}")
        return self.last_exec_result

    def show_last_exec_result(self, fields: str) -> str:
        if not self.last_exec_result:
            return ''
        Record = namedtuple('Record', fields)
        curs_str = ''
        for rec in map(Record._make, self.last_exec_result):
            for fnum, fval in enumerate(rec):
                if isinstance(fval, str):
                    fval = f'"{fval}"'
                elif isinstance(fval, datetime.datetime):
                    fval = f'({fval})'
                curs_str += f"{rec._fields[fnum]}={fval}  "
            curs_str += "\n"
        return curs_str


class AuxSQL:
    """
    auxiliary functions for MySQL
    """

    @staticmethod
    def datetime_2_sql(dt: datetime.datetime) -> str:
        """ python datetime.datetime --> MySQL datetime """
        return dt.strftime('%Y-%m-%d %H:%M:%S')

    @classmethod
    def now_timestamp(cls) -> str:
        """ python datetime.datetime.now() --> sql datetime """
        return cls.datetime_2_sql(datetime.datetime.now())

    @staticmethod
    def bool_2_sql(val: bool) -> str:
        """ python bool -> sql bool """
        return str(val).upper()


if __name__ == '__main__':
    import time

    db = MyDb()
    db.exec('DROP TABLE IF EXISTS test_mydb')
    db.exec('CREATE TABLE test_mydb (time DATETIME, name VARCHAR(20), val INT, is_odd BOOL)')
    db.exec('DESCRIBE test_mydb')
    for i in range(5):
        db.exec(f'INSERT INTO test_mydb VALUES ('
                f'"{AuxSQL.now_timestamp()}",'
                f'"Just a Name","{i}", '
                f'{AuxSQL.bool_2_sql(i%2==1)})')
        time.sleep(1)  # to check datetime
    res = db.exec('SELECT * FROM test_mydb WHERE is_odd')

    print("received from db.exec:")
    print(f"{db.show_last_exec_result('time, name, val, is_odd')}")


    # print("\n".join([f"{ind}: time={line[0]} name={line[1]} "
    #                  f"val={line[1]} is_odd={bool(line[2])}" for ind, line in enumerate(res)]))

