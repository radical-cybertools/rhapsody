# these are unit tests for the 'ResourceManager' base class

import pytest

import rhapsody

def test_rm_fork():

    cfg = {'requested_nodes' : 3,
           'fake_resources'  : True}
    rm  = rhapsody.ResourceManager.get_instance(name='FORK', cfg=cfg)

    assert len(rm.nodelist) == 3


