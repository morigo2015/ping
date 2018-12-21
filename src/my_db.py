import datetime
from typing import List

import mysql.connector

_DB_PARAM = {
    'host': 'localhost',
    'user': 'root',
    'passwd': '121212',  # todo change it when security matters
    'database': 'mysql'
}


class MyDb:

    def __init__(self):
        self.db_param = _DB_PARAM
        self.mydb = mysql.connector.connect(
            host=self.db_param['host'], user=self.db_param['user'], passwd=self.db_param['passwd'],
            database=self.db_param['database']
            , autocommit=True
            , charset='latin1'
            # ,use_unicode=True
            # ,buffered=True
        )
        self.curs = self.mydb.cursor()
        print(f"connected to {self.db_param['user']}:{self.db_param['database']} host={self.db_param['host']}")

    def __del__(self):
        # self.curs.close()
        self.mydb.close()

    def exec(self, statement: str, args: List[str] = None, show=True) -> List[str]:
        if show:
            print(f"Exec:{statement}")
        try:
            self.curs.execute(statement, args)
        except  mysql.connector.Error as err:
            print(f"Something went wrong while exec {statement}: {format(err)}")
            raise
        result_str = [s for s in self.curs]
        if show and result_str:
            # print("result:"), "\n".join([s.__repr__() for s in result_str]))
            print("     results:")
            for ind, ln in enumerate(result_str):
                print(f"row {ind}: {ln}")
        return result_str

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
    tbl_name = 'images'
    db.exec('drop table if exists test_mydb')
    db.exec('create table test_mydb (name varchar(20), val float)')
    db.exec('describe test_mydb')
    db.exec('insert into test_mydb values ("aa",1), ("bb", 2)')
    res = db.exec('select * from test_mydb')
    print("received from db.exec:")
    print("\n".join([f"{ind}: name={line[0]} val={line[1]}" for ind, line in enumerate(res)]))
