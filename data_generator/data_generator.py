"""
Multi-Table Data Generator with Foreign Key Support

This module provides a flexible data generator that supports:
- Multiple tables in one configuration
- Foreign key relationships between tables
- 11+ column types
- Automatic dependency resolution
- Unique constraints
- Weighted choices
- CSV and JSON export

Author: Data Generator Team
Version: 2.0
"""

import random
import string
import json
import yaml
import csv
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union
from pathlib import Path


class MultiTableDataGenerator:
    """
    Advanced data generator with multi-table and foreign key support.

    Example:
        generator = MultiTableDataGenerator(seed=42)
        config = {
            'tables': [
                {'table_name': 'users', 'rows_count': 10, 'columns': [...]},
                {'table_name': 'orders', 'rows_count': 50, 'columns': [...]}
            ]
        }
        results = generator.generate_from_config(config)
    """

    def __init__(self, seed: Optional[int] = None):
        """
        Initialize the generator.

        Args:
            seed: Random seed for reproducibility
        """
        if seed is not None:
            random.seed(seed)
        self.used_values = {}
        self.generated_tables = {}

    def generate_from_config(self, config: Union[str, Dict]) -> Dict[str, Any]:
        """
        Generate data for multiple tables from configuration.

        Args:
            config: Either config dict or path to config file (YAML/JSON)

        Returns:
            Dictionary with all generated tables
        """
        # Load config if it's a file path
        if isinstance(config, str):
            config_path = Path(config)
            if config_path.suffix in ['.yaml', '.yml']:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
            elif config_path.suffix == '.json':
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
            else:
                raise ValueError("Config file must be .yaml, .yml, or .json")

        # Reset state
        self.generated_tables = {}

        # Handle both single table and multiple tables config
        if 'tables' in config:
            tables_config = config['tables']
            output_path = config.get('output_path', './output')
            output_format = config.get('output_format', 'csv')
        elif 'table_name' in config:
            tables_config = [config]
            output_path = config.get('output_path', './output')
            output_format = config.get('output_format', 'csv')
        else:
            raise ValueError("Config must have either 'tables' or 'table_name'")

        # Sort tables by dependencies
        sorted_tables = self._sort_tables_by_dependencies(tables_config)

        # Generate each table
        results = {}
        for table_config in sorted_tables:
            table_name = table_config['table_name']
            print(f"\n{'=' * 80}")
            print(f"Generating table: {table_name}")
            print('=' * 80)

            result = self._generate_single_table(table_config, output_path, output_format)
            results[table_name] = result

        print(f"\n{'=' * 80}")
        print("ALL TABLES GENERATED SUCCESSFULLY")
        print('=' * 80)
        for table_name, result in results.items():
            print(f"  📊 {table_name}: {result['rows_count']} rows")
        print('=' * 80)

        return results

    def _sort_tables_by_dependencies(self, tables_config: List[Dict]) -> List[Dict]:
        """Sort tables based on foreign key dependencies."""
        dependencies = {}
        table_map = {t['table_name']: t for t in tables_config}

        for table in tables_config:
            table_name = table['table_name']
            deps = set()

            for col in table.get('columns', []):
                if col.get('type') == 'reference':
                    ref_table = col.get('ref_table')
                    if ref_table:
                        deps.add(ref_table)

                col_name = col.get('name', '')
                if '.' in col_name:
                    ref_table = col_name.split('.')[0]
                    deps.add(ref_table)

            dependencies[table_name] = deps

        # Topological sort
        sorted_tables = []
        visited = set()

        def visit(table_name):
            if table_name in visited:
                return
            visited.add(table_name)
            for dep in dependencies.get(table_name, set()):
                if dep in table_map:
                    visit(dep)
            if table_name in table_map:
                sorted_tables.append(table_map[table_name])

        for table_name in dependencies:
            visit(table_name)

        return sorted_tables

    def _generate_single_table(self, table_config: Dict, output_path: str, output_format: str) -> Dict:
        """Generate data for a single table."""
        table_name = table_config['table_name']
        rows_count = table_config.get('rows_count', 100)
        columns = table_config.get('columns', [])

        table_output_path = table_config.get('output_path', output_path)
        table_output_format = table_config.get('output_format', output_format)

        self.used_values = {
            col['name']: set()
            for col in columns
            if col.get('unique', False)
        }

        print(f"Generating {rows_count} rows...")
        data = []
        for i in range(rows_count):
            row = {}
            for col in columns:
                row[col['name']] = self._generate_value(col, table_name)
            data.append(row)

            if (i + 1) % max(1, rows_count // 10) == 0:
                progress = ((i + 1) / rows_count) * 100
                print(f"   Progress: {progress:.0f}% ({i + 1}/{rows_count} rows)")

        print(f"Generated {len(data)} rows")

        # Store for references
        self.generated_tables[table_name] = data

        # Export
        self._export_data(data, table_name, table_output_format, table_output_path)

        return {
            'table_name': table_name,
            'rows_count': len(data),
            'columns': [col['name'] for col in columns],
            'data': data
        }

    def _generate_value(self, col: Dict[str, Any], current_table: str) -> Any:
        """Generate a single value, handling references."""
        col_type = col['type'].lower()
        col_name = col['name']
        unique = col.get('unique', False)

        # Handle reference type
        if col_type == 'reference':
            return self._generate_reference(col)

        # Handle table.column pattern
        if '.' in col_name:
            return self._generate_reference_from_name(col_name)

        # Regular column types
        if col_type == 'integer':
            return self._generate_integer(col, unique, col_name)
        elif col_type == 'float':
            return self._generate_float(col, unique, col_name)
        elif col_type == 'string':
            return self._generate_string(col, unique, col_name)
        elif col_type == 'choice':
            return self._generate_choice(col)
        elif col_type == 'boolean':
            return self._generate_boolean(col)
        elif col_type == 'date':
            return self._generate_date(col)
        elif col_type == 'datetime':
            return self._generate_datetime(col)
        elif col_type == 'email':
            return self._generate_email(col, unique, col_name)
        elif col_type == 'phone':
            return self._generate_phone(col)
        elif col_type == 'uuid':
            return self._generate_uuid()
        else:
            raise ValueError(f"Unsupported column type: {col_type}")

    def _generate_reference(self, col: Dict) -> Any:
        """Generate value from referenced table."""
        ref_table = col.get('ref_table')
        ref_column = col.get('ref_column')

        if not ref_table or not ref_column:
            raise ValueError(f"Reference column must have 'ref_table' and 'ref_column'")

        if ref_table not in self.generated_tables:
            raise ValueError(f"Referenced table '{ref_table}' not generated yet")

        ref_data = self.generated_tables[ref_table]
        if not ref_data:
            raise ValueError(f"Referenced table '{ref_table}' is empty")

        ref_values = [row[ref_column] for row in ref_data if ref_column in row]
        if not ref_values:
            raise ValueError(f"Column '{ref_column}' not found in table '{ref_table}'")

        return random.choice(ref_values)

    def _generate_reference_from_name(self, col_name: str) -> Any:
        """Generate value from table.column format in name."""
        parts = col_name.split('.')
        if len(parts) != 2:
            raise ValueError(f"Invalid reference format: {col_name}")

        ref_table, ref_column = parts

        if ref_table not in self.generated_tables:
            raise ValueError(f"Referenced table '{ref_table}' not generated yet")

        ref_data = self.generated_tables[ref_table]
        ref_values = [row[ref_column] for row in ref_data if ref_column in row]

        if not ref_values:
            raise ValueError(f"Column '{ref_column}' not found in table '{ref_table}'")

        return random.choice(ref_values)

    def _ensure_unique(self, value: Any, col_name: str, generator_func, max_attempts: int = 1000) -> Any:
        """Ensure the generated value is unique."""
        attempts = 0
        while value in self.used_values[col_name] and attempts < max_attempts:
            value = generator_func()
            attempts += 1

        if attempts >= max_attempts:
            raise ValueError(f"Could not generate unique value for '{col_name}'")

        self.used_values[col_name].add(value)
        return value

    def _generate_integer(self, col: Dict, unique: bool, col_name: str) -> int:
        range_vals = col.get('range', [0, 100])

        def gen():
            return random.randint(range_vals[0], range_vals[1])

        value = gen()
        if unique:
            value = self._ensure_unique(value, col_name, gen)
        return value

    def _generate_float(self, col: Dict, unique: bool, col_name: str) -> float:
        range_vals = col.get('range', [0.0, 100.0])
        decimals = col.get('decimals', 2)

        def gen():
            return round(random.uniform(range_vals[0], range_vals[1]), decimals)

        value = gen()
        if unique:
            value = self._ensure_unique(value, col_name, gen)
        return value

    def _generate_string(self, col: Dict, unique: bool, col_name: str) -> str:
        length = col.get('length', 10)
        prefix = col.get('prefix', '')
        suffix = col.get('suffix', '')
        chars = col.get('chars', string.ascii_letters + string.digits)

        def gen():
            random_str = ''.join(random.choices(chars, k=length))
            return f"{prefix}{random_str}{suffix}"

        value = gen()
        if unique:
            value = self._ensure_unique(value, col_name, gen)
        return value

    def _generate_choice(self, col: Dict) -> Any:
        values = col.get('values', [])
        if not values:
            raise ValueError(f"Column '{col['name']}' requires 'values' list")
        weights = col.get('weights', None)
        return random.choices(values, weights=weights, k=1)[0]

    def _generate_boolean(self, col: Dict) -> bool:
        true_probability = col.get('true_probability', 0.5)
        return random.random() < true_probability

    def _generate_date(self, col: Dict) -> str:
        date_format = col.get('format', '%Y-%m-%d')
        if 'range' in col:
            start_str, end_str = col['range']
            start_date = datetime.strptime(start_str, date_format)
            end_date = datetime.strptime(end_str, date_format)
            days_between = (end_date - start_date).days
            random_days = random.randint(0, days_between)
            random_date = start_date + timedelta(days=random_days)
        else:
            random_date = datetime.now() - timedelta(days=random.randint(0, 365))
        return random_date.strftime(date_format)

    def _generate_datetime(self, col: Dict) -> str:
        dt_format = col.get('format', '%Y-%m-%d %H:%M:%S')
        if 'range' in col:
            start_str, end_str = col['range']
            start_dt = datetime.strptime(start_str, dt_format)
            end_dt = datetime.strptime(end_str, dt_format)
            seconds_between = int((end_dt - start_dt).total_seconds())
            random_seconds = random.randint(0, seconds_between)
            random_dt = start_dt + timedelta(seconds=random_seconds)
        else:
            random_dt = datetime.now() - timedelta(seconds=random.randint(0, 31536000))
        return random_dt.strftime(dt_format)

    def _generate_email(self, col: Dict, unique: bool, col_name: str) -> str:
        domains = col.get('domains', ['example.com'])

        def gen():
            username_length = random.randint(5, 15)
            username = ''.join(random.choices(string.ascii_lowercase + string.digits, k=username_length))
            domain = random.choice(domains)
            return f"{username}@{domain}"

        value = gen()
        if unique:
            value = self._ensure_unique(value, col_name, gen)
        return value

    def _generate_phone(self, col: Dict) -> str:
        pattern = col.get('pattern', '###-###-####')
        return ''.join(random.choice(string.digits) if c == '#' else c for c in pattern)

    def _generate_uuid(self) -> str:
        import uuid
        return str(uuid.uuid4())

    def _export_data(self, data: List[Dict], table_name: str, output_format: str, output_path: str) -> None:
        """Export data in specified format(s)."""
        output_dir = Path(output_path)
        output_dir.mkdir(parents=True, exist_ok=True)

        formats = output_format.split(',') if ',' in output_format else [output_format]

        for fmt in formats:
            fmt = fmt.strip().lower()
            if fmt == 'csv':
                self._export_csv(data, output_dir / f"{table_name}.csv")
            elif fmt == 'json':
                self._export_json(data, output_dir / f"{table_name}.json")
            elif fmt == 'both':
                self._export_csv(data, output_dir / f"{table_name}.csv")
                self._export_json(data, output_dir / f"{table_name}.json")

    def _export_csv(self, data: List[Dict], filepath: Path) -> None:
        """Export to CSV."""
        if not data:
            return
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        print(f"Exported to: {filepath}")

    def _export_json(self, data: List[Dict], filepath: Path) -> None:
        """Export to JSON."""
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        print(f"Exported to: {filepath}")

    def print_sample(self, table_name: str, n: int = 5) -> None:
        """Print sample data from a table."""
        if table_name not in self.generated_tables:
            print(f"Table '{table_name}' not found")
            return

        data = self.generated_tables[table_name]
        print(f"\n{'=' * 80}")
        print(f"SAMPLE DATA: {table_name} (First {min(n, len(data))} rows)")
        print('=' * 80)
        for i, row in enumerate(data[:n], 1):
            print(f"\nRow {i}:")
            for key, value in row.items():
                print(f"  {key:25s}: {value}")
        print('=' * 80)

    def get_dataframe(self, table_name: str):
        """
        Get table data as pandas DataFrame (if pandas is available).

        Args:
            table_name: Name of the table

        Returns:
            pandas DataFrame or None if pandas not available
        """
        try:
            import pandas as pd
            if table_name in self.generated_tables:
                return pd.DataFrame(self.generated_tables[table_name])
            else:
                print(f"Table '{table_name}' not found")
                return None
        except ImportError:
            print("pandas not installed. Install with: pip install pandas")
            return None