# ping-saver
"""
Get ping info for each time interval and save it to MySQL table.
"""

import subprocess
from typing import List, Dict, Union, Any

from my_db import MyDb, AuxSQL
from interv_timer import IntervTimer

HOSTS = {
    'external': 'www.ua',
    'router': '192.168.1.1',
    'old_hik': '192.168.1.64',
    'bullet': '192.168.1.70',
    'door_bell': '192.168.1.165'
}
SLEEP_INTERV_SEC = 60.0


class DbPing(MyDb):
    """ actions with ping table in MySql """

    def __init__(self):
        super().__init__()

    def create_ping_table(self, show=True):
        query = """
            create table if not exists ping (
              host varchar(20),
              time datetime,
              ok bool,
              loss float,
              min_rtt float,
              avg_rtt float,
              max_rtt float,
              mdev_rtt float
              )
              """
        self.exec(query, show=show)

    def count_ping_items(self) -> int:
        query = """
            select count(*)
            from ping
        """
        res = self.exec(query, show=False)
        return int(res[0][0])  # there is 1-line/1-column in res

    def insert_ping_item(self, ping_res: Dict[str, Any], show=False):
        query = f"""
            insert into ping values(
                '{ping_res['host']}', '{ping_res['time']}', {AuxSQL.bool_2_sql(ping_res['OK'])},
                '{ping_res['loss(%)']}', 
                '{ping_res['min']}','{ping_res['avg']}','{ping_res['max']}','{ping_res['mdev']}'
            )
        """
        # print(query)
        self.exec(query, show=show)


def ping(host: str, n: int = 3):
    """ ping host, parse output
    """
    results = { # by default - disconnected (to be changed if ping returns)
        'host': host, 'time': AuxSQL.now_timestamp(),
        'OK': False, 'loss(%)': 100, 'min': -1.0, 'avg': -1.0, 'max': -1.0, 'mdev': -1.0,  # disconnected
    }
    try:
        ping_answer: str = subprocess.check_output(
            [f'ping {host} -c {n} -q'],
            universal_newlines=True,
            shell=True,
            stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        # results['OK'] = False
        # by default the results have already been set to disconnected
        pass
    else:
        results['OK'] = True
        answer_lines = ping_answer.splitlines(keepends=False)
        # penultimate line - take losses percentage
        results['loss(%)'] = int(answer_lines[-2].split(sep=' ')[5][:-1])
        # last line - take rtt indicators
        rtt_indicators = answer_lines[-1].split(sep=' ')[1].split(sep='/')
        rtt_values = answer_lines[-1].split(sep=' ')[3].split(sep='/')
        for indicator, value in zip(rtt_indicators, rtt_values):
            results[indicator] = float(value)
    return results


def main():
    db = DbPing()
    db.create_ping_table(show=False)
    print(f' There are {db.count_ping_items()} items in ping table')
    it = IntervTimer(SLEEP_INTERV_SEC)  # interval timer to keep interval between awakes regardless of delays
    print(f'{"time":^20s} : {"host":^15s} : {"OK"} :{"loss(%)":5s}: {"avg":^5s}')
    while True:
        for host_name in HOSTS:
            res = ping(HOSTS[host_name])
            db.insert_ping_item(res, show=False)
            print(f'{res["time"]:20s} : {res["host"]:.>15s} : '
                  f'{"+" if res["OK"] else "-"} : {res["loss(%)"]:5.1f} : {res["avg"]:5.1f}')
        it.wait_interval()


if __name__ == '__main__':
    main()
