
import threading as mt

from typing import Optional

import radical.utils as ru

from . import NAMESPACE_SHORT as NS
from .utils import Proxy

SESSION_DESCRIPTION_PATH = './session.json'


class SessionDescription(ru.TypedDict):  # TODO: merge with Session?!

    _schema = {
        'uid': str,
        'proxy_addr': str,
        'registry_addr': str,
        'runtime': int,
    }

    _defaults = {}

    def __init__(self, *args, **kwargs):
        if not kwargs:
            kwargs = ru.read_json(args[0] if args else SESSION_DESCRIPTION_PATH)
        super().__init__(from_dict=kwargs)


class Session:

    def __init__(self, uid: Optional[str] = None, load: bool = False):
        options = {}
        if not load:
            options['uid'] = uid or \
                             ru.generate_id(f'{NS}.session', mode=ru.ID_PRIVATE)
        self._description = SessionDescription(**options)

        self._proxy = None
        self._registry = None

    def close(self):
        if self._proxy:
            self._proxy.request('unregister', {'sid': self.uid})

    @property
    def uid(self):
        return self._description.uid

    @property
    def proxy_addr(self):
        return self._description.proxy_addr

    @proxy_addr.setter
    def proxy_addr(self, v):
        self._description.proxy_addr = v

    @property
    def registry_addr(self):
        return self._description.registry_addr

    @registry_addr.setter
    def registry_addr(self, v):
        self._description.registry_addr = v

    def _service_start(self, service, options: dict = None):
        ready = mt.Event()
        service_thread = mt.Thread(target=self._service_run,
                                   args=(service, ready, options or {}),
                                   daemon=True)
        service_thread.start()
        ready.wait()

    def _service_run(self, service, ready: mt.Event, options: dict):
        s = service(**options)
        try:
            s.start()
            setattr(self, f'{service.__class__.__name__.lower()}_addr', s.addr)
            ready.set()
            # run forever until process is interrupted or killed
            s.wait()
        finally:
            s.stop()
            s.wait()

    def proxy_start(self):
        """Starts ZMQ Proxy via which Coordinator will connect to Client.
        """
        if not self._registry:
            self.registry_start()

        self._service_start(Proxy)
        self._proxy = ru.zmq.Client(url=self.proxy_addr)

        channels = self._proxy.request('register', {'sid': self.uid})
        for channel, cfg in channels.items():
            self._registry['bridges.%s' % channel] = cfg

    def registry_start(self):
        self._service_start(ru.zmq.Registry, {'uid': f'{self.uid}',
                                              'path': self._cfg.path})  # TODO
        self._registry = ru.zmq.RegistryClient(url=self.registry_addr)



