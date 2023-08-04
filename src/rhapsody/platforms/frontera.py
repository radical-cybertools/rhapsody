
from ._base import PlatformDescription, ResourceManager


frontera = PlatformDescription(**{
    'resource_manager': ResourceManager.SLURM,
    'partition': 'normal',
    'env_setup': [
        'module unload intel impi',
        'module load   intel impi',
        'module load   python3/3.9.2'
    ],
    'work_dir': '$SCRATCH',
    'cores_per_node': 56,
    'fqdn': 'frontera.tacc.utexas.edu',
    'docs_url': 'https://frontera-portal.tacc.utexas.edu/user-guide'
})

