# RaDex:

`rhapsody.backends.data` can be used inside Radical Data Exchanger [RaDex](https://radical-cybertools.github.io/radex/). The RaDex imports — `Endpoint.serialize()` to use it for moving `scalars` and `tensors` between the heterogeneous components of an HPC-AI workflow — MPI simulations and Python functions without going through a filesystem and what you build from it is entirely up to you. 

The [examples](https://github.com/radical-cybertools/rhapsody/tree/main/examples/data) directory has both styles side by side: `00`/`01` use RaDex's typed clients, `02`/`03` use plain `redis-py`/native `dragon.data.ddict.DDict` — same infrastructure, different client.


# OpenFoam via RaDex:
The RHAPSODY OpenFOAM plugin lets you define and run OpenFOAM cases as RHAPSODY workflows. For more information please visit the main repo to setup and run the plugin here [radex-openfoam-plugin](https://github.com/CrayLabs/rhapsody-plugins-openfoam/tree/main)
