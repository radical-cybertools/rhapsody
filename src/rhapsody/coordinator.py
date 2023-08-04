
from .session import Session


class Coordinator:

    def __init__(self):
        self._session = Session(load=True)
        self._session.registry_start()

    @property
    def session(self):
        return self._session

    def close(self):
        self._session.close()

