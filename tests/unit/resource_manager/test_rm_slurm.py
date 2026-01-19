#!/usr/bin/env python3

import os
import glob

import pytest
import rhapsody


def test_rm_slurm():
    pwd = os.path.dirname(os.path.abspath(__file__))

    # make a copy of os.environ to restore later
    old_env = os.environ.copy()

    # for each slurm test case in test_cases/*_slurm_*.env:
    #   - set the env
    #   - run the test
    #   - restore the env
    for env_file in glob.glob(f"{pwd}/test_cases/*_slurm_*.env"):
        try:
            # read the env file and set os.environ
            with open(env_file) as f:
                for line in f:
                    key, value = line.strip().split("=", 1)
                    os.environ[key] = value

            rm = rhapsody.ResourceManager.get_instance(name="SLURM")

            assert len(rm.node_list) == int(os.environ["SLURM_NNODES"])

        finally:
            # restore the env
            os.environ.clear()
            os.environ.update(old_env)


if __name__ == "__main__":
    test_rm_slurm()
