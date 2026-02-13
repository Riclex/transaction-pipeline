"""Data lineage tracking for the transaction pipeline.

Tracks data flow from source to output, enabling traceability,
impact analysis, and audit compliance.
"""

from __future__ import annotations

import json
import logging
import hashlib
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any
from uuid import uuid4

import pandas as pd

LOGGER = logging.getLogger(__name__)


@dataclass
class DataLineageNode:
    """Represents a node in the data lineage graph."""

    node_id: str
    node_type: str  # 'source', 'transformation', 'sink'
    name: str
    operation: str
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    input_schema: Optional[Dict[str, str]] = None
    output_schema: Optional[Dict[str, str]] = None
    row_count: Optional[int] = None
    file_hash: Optional[str] = None
    file_path: Optional[str] = None
    parent_nodes: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class LineageTracker:
    """Tracks data lineage throughout the pipeline."""

    def __init__(self, output_dir: Optional[Path] = None):
        """Initialize lineage tracker.

        Args:
            output_dir: Directory to store lineage reports
        """
        self.nodes: Dict[str, DataLineageNode] = {}
        self.output_dir = output_dir or Path("data/lineage")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.run_id = str(uuid4())[:8]

    def _compute_file_hash(self, file_path: Path) -> Optional[str]:
        """Compute SHA-256 hash of file for provenance tracking.

        Returns None if file does not exist.
        """
        if not file_path.exists():
            return None
        hash_sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()[:32]

    def _get_schema(self, df: pd.DataFrame) -> Dict[str, str]:
        """Extract schema from DataFrame."""
        return {col: str(dtype) for col, dtype in df.dtypes.items()}

    def register_source(
        self,
        name: str,
        file_path: Path,
        df: pd.DataFrame,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Register a data source.

        Args:
            name: Source name (e.g., 'raw_transactions')
            file_path: Path to source file
            df: DataFrame loaded from source
            metadata: Additional metadata

        Returns:
            node_id: Unique identifier for this node
        """
        node_id = f"{self.run_id}_source_{name}"
        node = DataLineageNode(
            node_id=node_id,
            node_type="source",
            name=name,
            operation="load",
            output_schema=self._get_schema(df),
            row_count=len(df),
            file_hash=self._compute_file_hash(file_path),
            file_path=str(file_path),
            metadata=metadata or {},
        )
        self.nodes[node_id] = node
        LOGGER.info("Registered source node: %s (%d rows)", node_id, len(df))
        return node_id

    def register_transformation(
        self,
        name: str,
        operation: str,
        input_df: pd.DataFrame,
        output_df: pd.DataFrame,
        parent_node_id: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Register a transformation step.

        Args:
            name: Transformation name
            operation: Operation type (e.g., 'normalize_status')
            input_df: Input DataFrame
            output_df: Output DataFrame
            parent_node_id: Parent node in lineage graph
            metadata: Additional metadata

        Returns:
            node_id: Unique identifier for this node
        """
        node_id = f"{self.run_id}_transform_{name}_{len(self.nodes)}"
        node = DataLineageNode(
            node_id=node_id,
            node_type="transformation",
            name=name,
            operation=operation,
            input_schema=self._get_schema(input_df),
            output_schema=self._get_schema(output_df),
            row_count=len(output_df),
            parent_nodes=[parent_node_id],
            metadata={
                **(metadata or {}),
                "input_rows": len(input_df),
                "output_rows": len(output_df),
                "row_delta": len(output_df) - len(input_df),
            },
        )
        self.nodes[node_id] = node
        LOGGER.info("Registered transformation: %s (%d -> %d rows)",
                   node_id, len(input_df), len(output_df))
        return node_id

    def register_sink(
        self,
        name: str,
        file_path: Path,
        df: pd.DataFrame,
        parent_node_id: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Register a data sink (output).

        Args:
            name: Sink name (e.g., 'ledger_output')
            file_path: Output file path
            df: DataFrame being written
            parent_node_id: Parent node in lineage graph
            metadata: Additional metadata

        Returns:
            node_id: Unique identifier for this node
        """
        node_id = f"{self.run_id}_sink_{name}"
        node = DataLineageNode(
            node_id=node_id,
            node_type="sink",
            name=name,
            operation="persist",
            input_schema=self._get_schema(df),
            row_count=len(df),
            file_path=str(file_path),
            parent_nodes=[parent_node_id],
            metadata=metadata or {},
        )
        self.nodes[node_id] = node
        LOGGER.info("Registered sink: %s (%d rows)", node_id, len(df))
        return node_id

    def get_lineage_for_node(self, node_id: str) -> List[DataLineageNode]:
        """Get full lineage chain for a specific node.

        Args:
            node_id: Node to trace back from

        Returns:
            List of nodes in lineage chain (from source to sink)
        """
        chain = []
        current_id = node_id
        visited = set()

        while current_id and current_id not in visited:
            visited.add(current_id)
            node = self.nodes.get(current_id)
            if node:
                chain.append(node)
                # Move to first parent (supports single parent for now)
                current_id = node.parent_nodes[0] if node.parent_nodes else None
            else:
                break

        return list(reversed(chain))

    def export_lineage(self, filename: Optional[str] = None) -> Path:
        """Export lineage graph to JSON.

        Args:
            filename: Output filename (default: lineage_{run_id}_{timestamp}.json)

        Returns:
            Path to exported file
        """
        if filename is None:
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            filename = f"lineage_{self.run_id}_{timestamp}.json"

        output_path = self.output_dir / filename

        lineage_data = {
            "run_id": self.run_id,
            "export_timestamp": datetime.now(timezone.utc).isoformat(),
            "total_nodes": len(self.nodes),
            "nodes": [asdict(node) for node in self.nodes.values()],
        }

        with open(output_path, "w") as f:
            json.dump(lineage_data, f, indent=2, default=str)

        LOGGER.info("Exported lineage to %s", output_path)
        return output_path

    def get_impact_analysis(self, source_node_id: str) -> List[str]:
        """Analyze downstream impact of a source node.

        Args:
            source_node_id: Source node to analyze

        Returns:
            List of downstream node IDs affected by this source
        """
        impacted = []
        for node_id, node in self.nodes.items():
            if source_node_id in node.parent_nodes:
                impacted.append(node_id)
                # Recursively find further downstream nodes
                impacted.extend(self.get_impact_analysis(node_id))
        return impacted


# Global lineage tracker instance
_lineage_tracker: Optional[LineageTracker] = None


def init_lineage_tracker(output_dir: Optional[Path] = None) -> LineageTracker:
    """Initialize global lineage tracker."""
    global _lineage_tracker
    _lineage_tracker = LineageTracker(output_dir)
    return _lineage_tracker


def get_lineage_tracker() -> Optional[LineageTracker]:
    """Get global lineage tracker instance."""
    return _lineage_tracker
