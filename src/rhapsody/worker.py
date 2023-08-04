
import radical.utils as ru


class Worker:

    def __init__(self, registry_addr):
        self._registry = ru.zmq.RegistryClient(url=registry_addr)

