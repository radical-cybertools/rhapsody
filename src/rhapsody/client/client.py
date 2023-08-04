
from ..session import Session


class Client:

    def __init__(self):
        self._session = Session()
        self._session.proxy_start()

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    @property
    def session(self):
        return self._session

    def close(self):
        self._session.close()

