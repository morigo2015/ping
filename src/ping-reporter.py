import datetime
import subprocess
import time

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.colors import LinearSegmentedColormap
import numpy as np
import pandas as pd

from my_db import MyDb

result_folder = '../tst'
PING_THRESHOLD = 25

# @formatter:off
host_info = {
    'www.ua':        ['External',  0],
    '192.168.1.1':   ['Router',    1],
    '192.168.1.64':  ['Camera:\nold_hik',   4],
    '192.168.1.70':  ['Camera:\nbullet',    3],
    '192.168.1.165': ['Camera:\ndoor_bell', 2],
}
# @formatter:on

# noinspection PyUnresolvedReferences
PandasFrame = pd.core.frame.DataFrame  # shortcut for annotations


class PltPing:
    """
    plot charts for ping data
    """
    def __init__(self):
        self.db = None  # connection to MySQL
        # SQL ->(get_ping_history)-> df_inp ->(prepare_data)-> df ->(scale_df)-> df_scales ->(draw..)-> df_.. -> charts
        self.df_inp: PandasFrame = None
        self.df: PandasFrame = None
        self.df_scaled: PandasFrame = None
        self.df_ok: PandasFrame = None
        self.df_ext_rtt: PandasFrame = None
        globals()['p'] = self

    def _get_ping_history(self):
        """ SQL table 'ping' --> self.df_inp
        """
        if self.db is None:
            self.db = MyDb()
        query = 'select * from ping order by time asc'
        self.df_inp = pd.read_sql(query, self.db.mydb)
        return

    def load_data(self):
        """ SQL table 'ping' --> self.dp_inp --> (data preparing) --> self.df
        """
        if self.df_inp is None:
            self._get_ping_history()
        self.df = self.df_inp.copy()

        # avg==-1 --> there was no answer from ping
        self.df.loc[self.df.avg_rtt == -1, 'avg_rtt'] = np.nan
        # df.avg_rtt.clip_upper(15.0, inplace=True)

        self.df.index = self.df['time']
        self.df.index = self.df.index.floor('1T')  # truncate to minutes

        # hosts --> columns
        self.df = self.df.groupby([self.df.index, 'host'])['ok', 'avg_rtt'].agg('mean').unstack()

        # sort columns (hosts_names) by host_info[host_name].seqn
        col = self.df.columns.tolist()
        # multiindex: level 0 - vars(ok,avg_rtt) level 1 - hosts;  reverse - to set external and router on top of chart
        col2 = sorted(col, key=lambda s: host_info[s[1]][1], reverse=True)
        self.df = self.df[col2]

        # host --> host_names
        self.df = self.df.rename(columns={h: host_info[h][0] for h in host_info})
        return

    # @formatter:off
    scales = {
        'hour':     {'resample': '1T', 'ticks': 60, 'format':"%H:%M",    'x_base':3.0, 'units':'minutes'},
        '24-hours': {'resample': '1H', 'ticks': 24, 'format':"%Hh",      'x_base':1.0, 'units':'hours'},
        'month':    {'resample': '1D', 'ticks': 30, 'format':"%Y-%m-%d", 'x_base':1.0, 'units':'days'},
    }
    # @formatter:on

    def scale_df(self, scale: str):
        """ self.df --> (scaling) --> self.dp_scaled
        """
        self.df_scaled = self.df.loc[:, ['ok', 'avg_rtt']].resample(self.scales[scale]['resample']).mean()
        self.df_scaled = self.df_scaled.tail(self.scales[scale]['ticks'])  # cut data out of reporting period
        return

    def draw_heat_ok(self, fig, ax, scale: str):
        self.df_ok = self.df_scaled['ok'].copy()

        # draw
        color_list = ["green", "olive", "darkkhaki", "orange", "red"][::-1]
        heatmap = ax.pcolor(self.df_ok.T,  # cm.get_cmap('viridis', 256))
                            cmap=LinearSegmentedColormap.from_list("", color_list),
                            vmin=np.nanmin(self.df_ok.T), vmax=np.nanmax(self.df_ok.T),
                            edgecolors='k', linewidth=1)
        # ax.patch.set(color='red')  # hatch='x',edgecolor='red',fill=True,
        # fig.colorbar(heatmap, extend='both')  #

        # set x axis
        ax.xaxis.set_major_locator(ticker.IndexLocator(self.scales[scale]['x_base'], 0.0))  # 0.5
        format = self.scales[scale]['format']
        ax.xaxis.set_major_formatter(ticker.FuncFormatter(
            lambda x, pos: f"{self.df_ok.index[np.clip(int(x), 0, len(self.df_ok.index) - 1)].strftime(format)}")
        )
        ax.tick_params(axis='x', labelrotation=90.)
        ax.set_xlabel(self.scales[scale]['units'])

        # set y axis (host_names ordered by host_info[seqn]
        ax.yaxis.set_major_locator(ticker.IndexLocator(1.0, 0.5))
        yformatter = lambda x, pos: f"{self.df_ok.columns[int(x)] if x < len(self.df_ok.columns) else '=No label='}"
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(yformatter))

    def draw_line_rtt(self, fig, ax, scale: str):
        # tune data
        self.df_ext_rtt = self.df_scaled['avg_rtt']['External'].copy()
        self.df_ext_rtt.clip_upper(PING_THRESHOLD, inplace=True)

        # draw
        # ax.plot(self.df_ext_rtt) #
        self.df_ext_rtt.plot.line(ax=ax, grid=True, use_index=False, title=f"Last {scale}")

        # set axes
        ax.xaxis.set_major_locator(ticker.IndexLocator(self.scales[scale]['x_base'], 0.5))
        ax.xaxis.set_major_formatter(ticker.NullFormatter())
        ax.tick_params(axis='x', labelrotation=90.)

        # set y axis
        ax.set_ylabel('Average ping\nto outside (in ms)')

    def write_stat(self):
        print("updatng stat.txt")
        with open(f"{result_folder}/stat.txt", "w") as f:
            msg = f"Charts created at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            f.write(msg)

    def draw_all(self):
        """ draw all charts, save results to files
        """
        self.load_data()
        for scale in self.scales:
            self.scale_df(scale)
            fig, (ax1, ax2) = plt.subplots(nrows=2)  # , sharex=True)
            self.draw_line_rtt(fig, ax1, scale)
            self.draw_heat_ok(fig, ax2, scale)
            fig.subplots_adjust(hspace=0)
            # plt.tight_layout()
            fname = f"{result_folder}/{scale}.png"
            fig.savefig(fname=fname)
            print(f"Figure for {scale} is saved to {fname}")
            # fig.show()
            plt.close(fig)
        self.write_stat()

    @staticmethod
    def update_server_files() -> bool:
        print('updating server files ...')
        cmd = f'/home/im/cloud/google-cloud-sdk/bin/gcloud compute scp * im@st:~/tst/'
        r = subprocess.check_call(cmd,cwd=result_folder,shell=True)
        # print(f"execute command {cmd} in cwd={result_folder}")
        return r==0

if __name__ == '__main__':
    print('start')
    p = PltPing()
    while True:
        p.draw_all()
        if p.update_server_files():
            print(f"{datetime.datetime.now()}: Remote files are updated. Waiting for next cycle...")
            time.sleep(600)
        else:
            print(f"File update failed.")
            break
