""" interval timer
"""
import time


class IntervTimer:
    """ wake at the start of each interval (regardless of time spent on wake handling)
    """

    def __init__(self, interval: float, logger=None):
        self.start = time.time()
        self.interval = interval
        self.next_awake = self.start + interval
        self.logger = logger
        if self.logger:
            self.logger(f"Init IntervTimer. Time(seconds)={time.time()}")

    def wait_interval(self):
        """ sleep till the start of the next interval
        """
        time_remains = self.next_awake - time.time()
        if time_remains > 0:  # we havn't skip start of next interval yet
            self.next_awake += self.interval
        else:  # assigned interval has started already, no reason to wait it
            # set awake to start of nearest next interval
            intervs_skipped = int(- time_remains / self.interval)  # how many entire intervals has been skipped
            self.next_awake += self.interval * (intervs_skipped + 1)
            time_remains = self.next_awake - time.time()
        time.sleep(time_remains)
        if self.logger:
            now = time.time()
            gap = (now - self.start) % self.interval
            self.logger(f"New awake, time.time(): {now}  From timer init: {now - self.start}  Gap:{gap:7.6f}\n")

    def enable_logger(self, logger):
        self.logger = logger

    def disable_logger(self):
        self.logger = None


if __name__ == "__main__":

    import random


    def test():
        test_steps = 3
        it = IntervTimer(10.0, logger=print)

        print('short sleeps')
        for iter in range(test_steps):
            print(f"iter={iter} of {test_steps}")
            time.sleep(3.0 + random.random() * 5.0)
            it.wait_interval()

        print('long sleeps, log to file')
        with open("time_interv.log", "w") as log:
            it2 = IntervTimer(10.0, logger=log.write)
            for iter in range(test_steps):
                print(f"iter={iter} of {test_steps}")
                time.sleep(3.0 + random.random() * 50.0)
                it2.wait_interval()


    test()
