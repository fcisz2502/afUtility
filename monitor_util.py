import sys
import traceback
import threading


# -----------------------------------------------------------------------------
class MyThread(threading.Thread):
    def __init__(self, email_ins, func, args=()):
        super(MyThread, self).__init__()
        self._email = email_ins
        self._func = func
        self._args = args

        self._res = None
        self._exitcode = 0
        self._exit_traceback = ''

    def run(self):
        try:
            self._res = self._func(*self._args)
        except Exception, e:
            print(repr(e))
            self._exitcode = 1
            self._exit_traceback = ''.join(traceback.format_exception(*sys.exc_info()))

        self._send()

    def _send(self):
        if self._exitcode:
            self._email.send(
                "%s failed!" % self._func.__name__,
                self._exit_traceback
            )

        elif isinstance(self._res, pd.DataFrame):
            # when self._res is DataFrame, use elif not self._res will raise ValueError
            # ValueError: The truth value of a DataFrame is ambiguous.
            #      Use a.empty, a.bool(), a.item(), a.any() or a.all().
            # a = pd.DataFrame()
            # if not a:
            #     print('True')
            # code as above will raise ValueError
            # so I use isinstance(self._res, pd.DataFrame) to catch pd.DataFrame return
            print("%s is done. Got a df as return." % self._func.__name__)
            print(self._res)

        elif not self._res or self._res is None:
            self._email.send(
                "Warning!",
                '%s has got no return or got False!' % self._func.__name__
            )
        else:
            print("%s is done. return is: %s." % (self._func.__name__, self._res))
            

# -----------------------------------------------------------------------------
# better use decorator instead of inherit, may change it later
class OrderReviewThread(MyThread):
    def __init__(self, redis_ins, email_ins, func, args=()):
        super(OrderReviewThread, self).__init__(email_ins, func, args)
        self._rcli = redis_ins

    def run(self):
        try:
            self._res = self._func(*self._args)
            self._rcli.set(
                'last_dt_tb_order_review',
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )
        except Exception, e:
            print(repr(e))
            self._exitcode = 1
            self._exit_traceback = ''.join(traceback.format_exception(*sys.exc_info()))

        self._send()


# -----------------------------------------------------------------------------
if __name__ == '__main__':
    pass
