#!/usr/bin/env python3
"""
Fix escaped docstrings in tpcds_dataframe_queries.py
"""

with open('tpcds_dataframe_queries.py', 'r') as f:
    content = f.read()

# Replace escaped triple quotes with regular triple quotes
content = content.replace(r'\"\"\"', '"""')

# Write back
with open('tpcds_dataframe_queries.py', 'w') as f:
    f.write(content)

print("Fixed docstring escaping in tpcds_dataframe_queries.py")