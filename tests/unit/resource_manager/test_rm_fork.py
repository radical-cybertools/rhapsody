#!/usr/bin/env python3

import pytest
import rhapsody


def test_rm_fork():
    cfg = rhapsody.RMConfig(requested_nodes=3, fake_resources=True)
    rm = rhapsody.ResourceManager.get_instance(name="FORK", cfg=cfg)

    assert len(rm.node_list) == 3

    # if fake_resources is False, an exception should be raised
    with pytest.raises(RuntimeError):
        cfg = rhapsody.RMConfig(requested_nodes=3, fake_resources=False)
        rm = rhapsody.ResourceManager.get_instance(name="FORK", cfg=cfg)


if __name__ == "__main__":
    test_rm_fork()
