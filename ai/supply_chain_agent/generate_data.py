"""Generate Supply Chain Data"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'data_generator'))

from data_generator import MultiTableDataGenerator

generator = MultiTableDataGenerator(seed=42)
results = generator.generate_from_config('supply_chain_config.yaml')

for table_name in results.keys():
    generator.print_sample(table_name, n=3)

print("\n✅ Generated files in: ./supply_chain_data/")
print("📄 inventory.csv, orders.csv, suppliers.csv")
