

import radical.utils as ru

from .utils import EnumTypes

TaskState = EnumTypes(
    ('NEW', 'new'),
    ('SCHEDULING_PENDING', 'scheduling_pending'),
    ('SCHEDULING', 'scheduling'),
    ('EXECUTING_PENDING', 'executing_pending'),
    ('EXECUTING', 'executing'),
    ('DONE', 'done'),
    ('FAILED', 'failed'),
    ('CANCELLED', 'cancelled')
)


class Task(ru.TypedDict):

    _schema = {}

    _defaults = {}

    def __init__(self):
        pass


class Workload(ru.TypedDict):

    _schema = {}

    _defaults = {}

    def __init__(self):
        pass

