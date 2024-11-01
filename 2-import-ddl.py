#!/usr/bin/env python3
"""
DDL Processing Utility
Handles DDL file processing, YAML configuration management, and slice distribution loading.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Any, Tuple

import yaml
from ddlparse import DdlParse  # Commented out as in original, but shown for completeness

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class TableColumn:
    """Represents a database table column with its properties."""
    name: str
    data_type: str
    length: int | None = None
    precision: int | None = None
    not_null: bool = False
    primary_key: bool = False
    unique: bool = False
    generator: str = "randomize"

@dataclass
class TableDefinition:
    """Represents a complete table definition."""
    data: List[TableColumn] = field(default_factory=list)

    def to_dict(self) -> Dict[str, List[Dict[str, Any]]]:
        """Convert the table definition to a dictionary format for YAML export."""
        return {
            "data": [
                {
                    "name": col.name,
                    "data_type": col.data_type,
                    "length": col.length,
                    "precision": col.precision,
                    "not_null": col.not_null,
                    "primary_key": col.primary_key,
                    "unique": col.unique,
                    "generator": col.generator
                }
                for col in self.data
            ]
        }

@dataclass
class Slice:
    """Represents a data slice with start and end values."""
    start: int
    end: int

    def __str__(self) -> str:
        return f"{self.start} | {self.end}"

class DDLProcessor:
    """Handles DDL file processing and conversion to YAML."""
    
    def __init__(self, ddl_parser: Any = DdlParse):
        """
        Initialize the DDL processor.
        
        Args:
            ddl_parser: DDL parsing class (default: DdlParse)
        """
        self.ddl_parser = ddl_parser
        
    def process_ddl_file(self, input_path: Path, output_path: Path) -> None:
        """
        Process a DDL file and convert it to YAML format.
        
        Args:
            input_path: Path to input DDL file
            output_path: Path to output YAML file
        """
        try:
            # Read DDL file
            ddl_content = self._read_file(input_path)
            
            # Parse DDL and create table definition
            table_def = self._parse_ddl(ddl_content)
            
            # Write to YAML
            self._write_yaml(table_def, output_path)
            logger.info(f"Successfully processed DDL from {input_path} to {output_path}")
            
        except Exception as e:
            logger.error(f"Error processing DDL file: {str(e)}")
            raise

    def _read_file(self, file_path: Path) -> str:
        """Read content from a file."""
        try:
            with open(file_path, 'r') as file:
                return file.read()
        except IOError as e:
            logger.error(f"Error reading file {file_path}: {str(e)}")
            raise

    def _parse_ddl(self, ddl_content: str) -> TableDefinition:
        """Parse DDL content and create a table definition."""
        table = self.ddl_parser().parse(ddl_content)
        table_def = TableDefinition()
        
        for col in table.columns.values():
            column = TableColumn(
                name=col.name,
                data_type=col.data_type,
                length=col.length,
                precision=col.precision,
                not_null=col.not_null,
                primary_key=col.primary_key,
                unique=col.unique
            )
            table_def.data.append(column)
        
        return table_def

    def _write_yaml(self, table_def: TableDefinition, output_path: Path) -> None:
        """Write table definition to YAML file."""
        try:
            with open(output_path, 'w') as outfile:
                yaml.dump(table_def.to_dict(), outfile, default_flow_style=False)
        except IOError as e:
            logger.error(f"Error writing YAML file {output_path}: {str(e)}")
            raise

class YAMLLoader:
    """Handles YAML file loading and processing."""
    
    @staticmethod
    def load_yaml(file_path: Path) -> Dict[str, Any]:
        """
        Load and parse a YAML file.
        
        Args:
            file_path: Path to YAML file
            
        Returns:
            Dictionary containing parsed YAML data
        """
        try:
            with open(file_path) as file:
                return yaml.safe_load(file)
        except Exception as e:
            logger.error(f"Error loading YAML file {file_path}: {str(e)}")
            raise

class SliceProcessor:
    """Handles slice distribution processing."""
    
    @staticmethod
    def load_slices(file_path: Path) -> List[Slice]:
        """
        Load slice distribution from a file.
        
        Args:
            file_path: Path to slices file
            
        Returns:
            List of Slice objects
        """
        slices = []
        try:
            with open(file_path) as f:
                for line in f:
                    start, end = SliceProcessor._parse_line(line)
                    slices.append(Slice(start, end))
            return slices
            
        except Exception as e:
            logger.error(f"Error loading slices from {file_path}: {str(e)}")
            raise

    @staticmethod
    def _parse_line(line: str) -> Tuple[int, int]:
        """Parse a line from the slices file into start and end values."""
        try:
            parts = line.rstrip("\n\r").split("|")
            return int(parts[1]), int(parts[2])
        except (IndexError, ValueError) as e:
            logger.error(f"Error parsing line '{line}': {str(e)}")
            raise

def main() -> None:
    """Main entry point for the application."""
    try:
        # Process DDL file
        # ddl_processor = DDLProcessor()
        # ddl_processor.process_ddl_file(
        #     Path("input.sql"),
        #     Path("output.yaml")
        # )
        
        # Load slices
        slice_processor = SliceProcessor()
        slices = slice_processor.load_slices(
            Path('90-Data-Definition-Language/3-slices.txt')
        )
        
        print("Slices Distribution")
        print("-" * 40)
        for slice_obj in slices:
            print(slice_obj)
            
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        raise

if __name__ == "__main__":
    main()
