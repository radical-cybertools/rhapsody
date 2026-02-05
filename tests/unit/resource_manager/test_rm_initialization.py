
import os
import pytest
from unittest.mock import MagicMock, patch, mock_open
import rhapsody
from rhapsody.resource_manager.base import Node

# -----------------------------------------------------------------------------
# Cobalt Initialization Tests
# -----------------------------------------------------------------------------

def test_cobalt_init_nodefile(tmp_path):
    """Test Cobalt initialization from COBALT_NODEFILE.
    
    Uses localhost cpu count as fallback if not derived from env (as per updated logic).
    """
    nodefile = tmp_path / "cobalt.nodes"
    nodefile.write_text("node1\nnode2\nnode3\n")
    
    env = {
        "COBALT_JOBID": "12345",
        "COBALT_NODEFILE": str(nodefile),
        "COBALT_JOBSIZE": "3" 
    }
    
    with patch.dict(os.environ, env):
        # Mock os.cpu_count to mimic having specific number of cores on localhost
        with patch("os.cpu_count", return_value=16):
            with patch("rhapsody.resource_manager.cobalt.Cobalt._check_nodes") as mock_check:
                mock_check.side_effect = lambda nodes: nodes
                
                rm = rhapsody.ResourceManager.get_instance("COBALT")
                assert len(rm.node_list) == 3
                assert rm.node_list[0].name == "node1"
                assert rm.info.cores_per_node == 16

def test_cobalt_init_missing_env():
    """Test Cobalt initialization fails without environment variables."""
    # cpu_count fallback works, but missing env vars should still raise RuntimeError 
    # about missing NODEFILE/PARTNAME
    with patch("os.cpu_count", return_value=16):
        with patch.dict(os.environ, {}, clear=True):
             with pytest.raises(RuntimeError) as excinfo:
                 rhapsody.ResourceManager.get_instance("COBALT")
             
             assert "RM COBALT creation failed" in str(excinfo.value)

# -----------------------------------------------------------------------------
# LSF Initialization Tests
# -----------------------------------------------------------------------------

def test_lsf_init_hostfile(tmp_path):
    """Test LSF initialization from LSB_DJOB_HOSTFILE."""
    hostfile = tmp_path / "lsf.hosts"
    hostfile.write_text("nodeA\nnodeA\nnodeB\nnodeB\n")
    
    env = {
        "LSB_JOBID": "555",
        "LSB_DJOB_HOSTFILE": str(hostfile)
    }
    
    with patch.dict(os.environ, env):
        with patch("rhapsody.resource_manager.lsf.LSF._check_nodes") as mock_check:
            mock_check.side_effect = lambda nodes: nodes
            
            rm = rhapsody.ResourceManager.get_instance("LSF")
            assert len(rm.node_list) == 2 
            assert rm.info.cores_per_node == 2 
            assert rm.node_list[0].name == "nodeA"
            assert rm.node_list[0].cores == 2

# -----------------------------------------------------------------------------
# Torque Initialization Tests
# -----------------------------------------------------------------------------

def test_torque_init_nodefile(tmp_path):
    """Test Torque initialization from PBS_NODEFILE."""
    nodefile = tmp_path / "pbs.nodes"
    nodefile.write_text("nodeX\nnodeX\nnodeY\nnodeY\n")
    
    env = {
        "PBS_JOBID": "999",
        "PBS_NODEFILE": str(nodefile),
        "PBS_NUM_NODES": "2" 
    }
    
    with patch.dict(os.environ, env):
        with patch("rhapsody.resource_manager.torque.Torque._check_nodes") as mock_check:
            mock_check.side_effect = lambda nodes: nodes
            
            rm = rhapsody.ResourceManager.get_instance("TORQUE")
            assert len(rm.node_list) == 2
            assert rm.info.cores_per_node == 2

# -----------------------------------------------------------------------------
# PBSPro Initialization Tests
# -----------------------------------------------------------------------------

def test_pbspro_init_vnodes():
    """Test PBSPro initialization parsing vnodes from qstat."""
    qstat_output = "exec_vnode = (node001:ncpus=4)+(node002:ncpus=4)"
    
    env = {"PBS_JOBID": "777"}
    
    with patch.dict(os.environ, env):
        with patch("rhapsody.resource_manager.pbspro.subprocess.run") as mock_run:
            mock_proc = MagicMock()
            mock_proc.stdout = qstat_output
            mock_proc.returncode = 0
            mock_run.return_value = mock_proc
            
            with patch("rhapsody.resource_manager.pbspro.PBSPro._check_nodes") as mock_check:
                 mock_check.side_effect = lambda nodes: nodes
                 
                 rm = rhapsody.ResourceManager.get_instance("PBSPRO")
                 assert len(rm.node_list) == 2
                 assert rm.info.cores_per_node == 4
                 assert rm.node_list[0].name == "node001"


def test_pbspro_init_fallback_nodefile(tmp_path):
    """Test PBSPro initialization falling back to PBS_NODEFILE if qstat fails."""
    nodefile = tmp_path / "fallback.nodes"
    # Using duplicates to imply 2 cores per node, so it auto-detects CPN=2.
    # If we used unique nodes, CPN would be 1 (default assumption in base parsing for uniform lists of singles)
    nodefile.write_text("node10\nnode10\nnode11\nnode11\n") 
    
    env = {
        "PBS_JOBID": "888",
        "PBS_NODEFILE": str(nodefile)
    }
    
    with patch.dict(os.environ, env):
        with patch("rhapsody.resource_manager.pbspro.subprocess.run", side_effect=ValueError("qstat failed parse")):
            with patch("rhapsody.resource_manager.pbspro.PBSPro._check_nodes") as mock_check:
                mock_check.side_effect = lambda nodes: nodes
                
                rm = rhapsody.ResourceManager.get_instance("PBSPRO")
                assert len(rm.node_list) == 2
                assert rm.info.cores_per_node == 2 # derived from duplicates

# -----------------------------------------------------------------------------
# Edge Cases & Error Handling
# -----------------------------------------------------------------------------

def test_check_nodes_ssh_failure():
    """Test _check_nodes removes nodes that fail SSH check."""
    from rhapsody.resource_manager.base import Node
    cfg = rhapsody.RMConfig(requested_nodes=2, fake_resources=True)
    rm = rhapsody.ResourceManager.get_instance("FORK", cfg=cfg)
    
    rm.info.node_list = [
        Node(name="good_node", index=0, cores=1, rm_info=rm.info),
        Node(name="bad_node", index=1, cores=1, rm_info=rm.info)
    ]
    
    def mock_subprocess_run(cmd, **kwargs):
        mock_ret = MagicMock()
        mock_ret.stdout = ""
        mock_ret.stderr = ""
        
        if "bad_node" in cmd:
            mock_ret.returncode = 1 
        else:
            mock_ret.returncode = 0 
        return mock_ret
        
    with patch("rhapsody.resource_manager.base.subprocess.run", side_effect=mock_subprocess_run):
        checked_nodes = rm._check_nodes(rm.info.node_list)
        
        assert len(checked_nodes) == 1
        assert checked_nodes[0].name == "good_node"


def test_check_nodes_all_fail():
    """Test _check_nodes returns empty list/error if all nodes fail."""
    from rhapsody.resource_manager.base import Node
    cfg = rhapsody.RMConfig(requested_nodes=1, fake_resources=True)
    rm = rhapsody.ResourceManager.get_instance("FORK", cfg=cfg)
    rm.info.node_list = [
        Node(name="bad1", index=0, cores=1, rm_info=rm.info), 
        Node(name="bad2", index=1, cores=1, rm_info=rm.info)
    ]
    
    mock_fail = MagicMock()
    mock_fail.returncode = 1
    
    with patch("rhapsody.resource_manager.base.subprocess.run", return_value=mock_fail):
        nodes = rm._check_nodes(rm.info.node_list)
        assert nodes == []


def test_heterogeneous_nodes_error(tmp_path):
    """Test initialization raises error for heterogeneous node cores."""
    hostfile = tmp_path / "hetero.hosts"
    hostfile.write_text("node1\nnode1\nnode2\nnode2\nnode2\n")
    
    env = {
        "LSB_JOBID": "111",
        "LSB_DJOB_HOSTFILE": str(hostfile)
    }
    
    with patch.dict(os.environ, env):
        with pytest.raises(RuntimeError) as excinfo:
             rhapsody.ResourceManager.get_instance("LSF")
        assert "RM LSF creation failed" in str(excinfo.value)
