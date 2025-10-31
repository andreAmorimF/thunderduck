#!/usr/bin/env python3
"""
Integrate the real DataFrame implementations into tpcds_dataframe_queries.py
"""

import re

# Read the implementations from implement_all_remaining.py
with open('implement_all_remaining.py', 'r') as f:
    content = f.read()

# Extract the IMPLEMENTATIONS string
match = re.search(r'IMPLEMENTATIONS = """(.*)"""', content, re.DOTALL)
if not match:
    print("Could not find implementations in implement_all_remaining.py")
    exit(1)

implementations = match.group(1)

# Read the current tpcds_dataframe_queries.py
with open('tpcds_dataframe_queries.py', 'r') as f:
    current = f.read()

# Find and replace each placeholder implementation
queries_to_replace = [25, 26, 29, 32, 37, 40, 41, 42, 43, 45, 48, 50, 52, 55, 62, 71, 72, 82, 84, 85, 91, 92, 96, 98, 99]

for q in queries_to_replace:
    # Find the implementation in the extracted text
    pattern = rf'@staticmethod\s+def q{q}\(spark: SparkSession\) -> DataFrame:(.*?)(?=@staticmethod|$)'
    impl_match = re.search(pattern, implementations, re.DOTALL)

    if impl_match:
        new_impl = f"    @staticmethod\n    def q{q}(spark: SparkSession) -> DataFrame:{impl_match.group(1)}"

        # Find and replace in current file
        old_pattern = rf'    @staticmethod\s+def q{q}\(spark: SparkSession\) -> DataFrame:.*?(?=    @staticmethod|# Dictionary|$)'
        current = re.sub(old_pattern, new_impl + "\n", current, flags=re.DOTALL)
        print(f"Replaced implementation for Q{q}")
    else:
        print(f"Could not find implementation for Q{q}")

# Write back
with open('tpcds_dataframe_queries.py', 'w') as f:
    f.write(current)

print("\nSuccessfully integrated real implementations into tpcds_dataframe_queries.py")