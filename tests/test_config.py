"""
Tests for configuration schema validation and YAML serialization.
"""

from pathlib import Path
import tempfile
import pytest
from pydantic import ValidationError

from src.config import (
    NodeConfig,
    PoolConfig,
    PortRange,
    ResiliencePolicy,
    TelemetryConfig,
)


def test_port_range_valid() -> None:
    pr = PortRange(start=9222, end=9266)
    assert pr.total_slots == 45
    assert pr.port_for_slot(0) == 9222
    assert pr.port_for_slot(44) == 9266
    assert pr.contains_port(9240) is True
    assert pr.contains_port(9200) is False
    assert pr.contains_port(9300) is False


def test_port_range_invalid_bounds() -> None:
    with pytest.raises(ValidationError):
        PortRange(start=9266, end=9222)  # start > end

    with pytest.raises(ValidationError):
        PortRange(start=80, end=90)  # below 1024


def test_port_range_slot_index_error() -> None:
    pr = PortRange(start=9222, end=9230)
    with pytest.raises(IndexError):
        pr.port_for_slot(10)  # total slots is 9 (0..8)


def test_node_config_validation() -> None:
    node = NodeConfig(
        node_id="node_00",
        slot_index=0,
        cdp_port=9222,
        user_data_dir=Path("/var/run/browser_pool/profile_00"),
    )
    assert node.node_id == "node_00"
    assert node.cdp_port == 9222
    assert node.headless is True
    assert node.active is True


def test_node_config_empty_id() -> None:
    with pytest.raises(ValidationError):
        NodeConfig(
            node_id="  ",
            slot_index=0,
            cdp_port=9222,
            user_data_dir=Path("/tmp/p"),
        )


def test_pool_config_generation() -> None:
    cfg = PoolConfig(pool_size=45, port_range=PortRange(start=9222, end=9266))
    nodes = cfg.generate_nodes()
    assert len(nodes) == 45
    assert nodes[0].node_id == "node_00"
    assert nodes[0].cdp_port == 9222
    assert nodes[44].node_id == "node_44"
    assert nodes[44].cdp_port == 9266


def test_pool_config_port_shortage() -> None:
    # 10 slots provided but pool_size is 45
    with pytest.raises(ValidationError):
        PoolConfig(pool_size=45, port_range=PortRange(start=9222, end=9231))


def test_pool_config_duplicate_ports() -> None:
    # Explicit node list with duplicate port
    node1 = NodeConfig(node_id="node_00", slot_index=0, cdp_port=9222, user_data_dir=Path("/tmp/1"))
    node2 = NodeConfig(node_id="node_01", slot_index=1, cdp_port=9222, user_data_dir=Path("/tmp/2"))
    with pytest.raises(ValidationError):
        PoolConfig(pool_size=2, port_range=PortRange(start=9222, end=9225), nodes=[node1, node2])


def test_pool_config_yaml_roundtrip() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        yaml_path = Path(tmpdir) / "test_pool.yaml"
        cfg_original = PoolConfig(pool_size=10, port_range=PortRange(start=9222, end=9231))
        cfg_original.generate_nodes()
        cfg_original.to_yaml(yaml_path)

        assert yaml_path.exists()
        loaded = PoolConfig.from_yaml(yaml_path)

        assert loaded.pool_size == 10
        assert len(loaded.nodes) == 10
        assert loaded.nodes[0].node_id == "node_00"
        assert loaded.nodes[0].cdp_port == 9222
        assert loaded.nodes[9].cdp_port == 9231


def test_explicit_node_outside_port_range_rejected() -> None:
    node = NodeConfig(node_id="n0", slot_index=0, cdp_port=9999, user_data_dir="/tmp/x")
    with pytest.raises(ValueError, match="outside port_range"):
        PoolConfig(pool_size=1, port_range=PortRange(start=9222, end=9222), nodes=[node])
