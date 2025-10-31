#!/usr/bin/env python3
"""
Script to add remaining DataFrame query implementations to tpcds_dataframe_queries.py
"""

import re

def extract_implementations():
    """Extract the implementations from implement_all_remaining.py"""
    with open('implement_all_remaining.py', 'r') as f:
        content = f.read()

    # Find the IMPLEMENTATIONS string
    match = re.search(r'IMPLEMENTATIONS = """(.*)"""', content, re.DOTALL)
    if match:
        return match.group(1)
    return None

def update_queries_file():
    """Update tpcds_dataframe_queries.py with new implementations"""

    # Read the existing file
    with open('tpcds_dataframe_queries.py', 'r') as f:
        lines = f.readlines()

    # Find where to insert (before the placeholder methods)
    insert_index = None
    for i, line in enumerate(lines):
        if 'def q25(spark: SparkSession) -> DataFrame:' in line:
            insert_index = i
            break

    if insert_index is None:
        print("Could not find insertion point")
        return

    # Get implementations
    implementations = extract_implementations()
    if implementations is None:
        print("Could not extract implementations")
        return

    # Remove the placeholder methods for q25-q99
    end_index = None
    for i in range(insert_index, len(lines)):
        if '# Dictionary of compatible queries' in lines[i]:
            end_index = i
            break

    # Create new content
    new_lines = lines[:insert_index]
    new_lines.append(implementations)
    new_lines.append('\n')
    if end_index:
        new_lines.extend(lines[end_index:])

    # Update the QUERY_IMPLEMENTATIONS dictionary at the end
    dict_start = None
    for i, line in enumerate(new_lines):
        if 'QUERY_IMPLEMENTATIONS: Dict[int, Callable] = {' in line:
            dict_start = i
            break

    if dict_start:
        # Find the closing brace
        dict_end = None
        for i in range(dict_start, len(new_lines)):
            if new_lines[i].strip() == '}':
                dict_end = i
                break

        if dict_end:
            # Replace with complete mapping
            new_dict_lines = ['QUERY_IMPLEMENTATIONS: Dict[int, Callable] = {\n']
            for q in [3, 7, 9, 12, 13, 15, 17, 19, 20, 25, 26, 29, 32, 37, 40, 41, 42, 43, 45, 48, 50, 52, 55, 62, 71, 72, 82, 84, 85, 91, 92, 96, 98, 99]:
                new_dict_lines.append(f'    {q}: TpcdsDataFrameQueries.q{q},\n')
            new_dict_lines.append('}\n')

            new_lines = new_lines[:dict_start] + new_dict_lines + new_lines[dict_end+1:]

    # Write back
    with open('tpcds_dataframe_queries.py', 'w') as f:
        f.writelines(new_lines)

    print("Successfully updated tpcds_dataframe_queries.py with all implementations")

if __name__ == "__main__":
    update_queries_file()