# these are unit tests for the 'ResourceManager' base class

import pytest

import rhapsody

def test_rm_fork():

    rm = rhapsody.ResourceManager.get_instance(name='FORK')

    assert len(rm.nodelist) == 1


