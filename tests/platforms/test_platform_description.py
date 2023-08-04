
import os

from unittest import TestCase

from rhapsody.platforms import PlatformDescription, ResourceManager
from rhapsody.platforms import frontera


class PlatformDescriptionTC(TestCase):

    def test_init_defaults(self):

        pd = PlatformDescription()

        self.assertEqual(pd.env_setup, [])
        self.assertEqual(pd.work_dir, os.path.expanduser('~'))
        self.assertEqual(pd.cores_per_node, 1)
        self.assertEqual(pd.gpus_per_node, 0)
        self.assertEqual(pd.hwthreads_per_core, 1)
        self.assertEqual(pd.cpus_per_node, 1)

    def test_frontera(self):

        pd = frontera

        self.assertEqual(pd.resource_manager, ResourceManager.SLURM)
        self.assertEqual(pd.work_dir, '$SCRATCH')
        self.assertEqual(pd.cores_per_node, 56)
        self.assertIsInstance(pd.fqdn, str)
        self.assertIsInstance(pd.docs_url, str)


