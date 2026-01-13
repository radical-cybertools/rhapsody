#!/usr/bin/env python3

import pytest
import rhapsody

from rhapsody import ResourceManager as RM

def test_rm_base():

    cfg = rhapsody.RMConfig(requested_nodes=3, fake_resources=True)
    with pytest.raises(NotImplementedError):
        rm = rhapsody.ResourceManager(cfg=cfg)

if __name__ == "__main__":

    test_rm_base()

