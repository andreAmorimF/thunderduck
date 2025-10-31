#!/usr/bin/env python3
"""
Analyze TPC-DS queries to determine which can be implemented in pure DataFrame API
"""

import os
import re
from typing import Dict, List, Tuple

def analyze_sql_features(sql_content: str) -> Dict[str, bool]:
    """Analyze SQL query for features that cannot be expressed in DataFrame API"""
    features = {
        # CTEs (Common Table Expressions) - WITH clause
        'has_cte': bool(re.search(r'\bWITH\s+\w+\s+AS\s*\(', sql_content, re.IGNORECASE)),

        # ROLLUP, CUBE, GROUPING SETS
        'has_rollup': bool(re.search(r'\bROLLUP\s*\(', sql_content, re.IGNORECASE)),
        'has_cube': bool(re.search(r'\bCUBE\s*\(', sql_content, re.IGNORECASE)),
        'has_grouping_sets': bool(re.search(r'\bGROUPING\s+SETS\s*\(', sql_content, re.IGNORECASE)),
        'has_grouping_func': bool(re.search(r'\bGROUPING\s*\(', sql_content, re.IGNORECASE)),

        # EXISTS/NOT EXISTS subqueries
        'has_exists': bool(re.search(r'\bEXISTS\s*\(', sql_content, re.IGNORECASE)),
        'has_not_exists': bool(re.search(r'\bNOT\s+EXISTS\s*\(', sql_content, re.IGNORECASE)),

        # Correlated subqueries (harder to detect, looking for common patterns)
        'has_correlated_subquery': bool(re.search(r'WHERE.*\(.*SELECT.*WHERE.*=.*\)', sql_content, re.IGNORECASE)),

        # INTERSECT/EXCEPT
        'has_intersect': bool(re.search(r'\bINTERSECT\b', sql_content, re.IGNORECASE)),
        'has_except': bool(re.search(r'\bEXCEPT\b', sql_content, re.IGNORECASE)),

        # UNION (can be done in DataFrame but complex with many)
        'has_union': bool(re.search(r'\bUNION\b', sql_content, re.IGNORECASE)),
        'union_count': len(re.findall(r'\bUNION\b', sql_content, re.IGNORECASE)),

        # Window functions (supported but complex)
        'has_window': bool(re.search(r'\bOVER\s*\(', sql_content, re.IGNORECASE)),

        # CASE statements (supported but can be complex)
        'has_case': bool(re.search(r'\bCASE\b', sql_content, re.IGNORECASE)),
        'case_count': len(re.findall(r'\bCASE\b', sql_content, re.IGNORECASE)),

        # Subqueries in FROM clause
        'has_subquery_from': bool(re.search(r'FROM\s*\([^)]*SELECT', sql_content, re.IGNORECASE)),

        # Complex date arithmetic
        'has_date_arithmetic': bool(re.search(r'date_add|date_sub|datediff|months_between', sql_content, re.IGNORECASE)),
    }
    return features

def is_dataframe_compatible(features: Dict[str, bool]) -> Tuple[bool, List[str]]:
    """Determine if query can be implemented in pure DataFrame API"""
    incompatible_features = []

    # Features that make a query incompatible with DataFrame API
    if features['has_cte']:
        incompatible_features.append('CTE (WITH clause)')
    if features['has_rollup']:
        incompatible_features.append('ROLLUP')
    if features['has_cube']:
        incompatible_features.append('CUBE')
    if features['has_grouping_sets']:
        incompatible_features.append('GROUPING SETS')
    if features['has_grouping_func']:
        incompatible_features.append('GROUPING() function')
    if features['has_exists']:
        incompatible_features.append('EXISTS subquery')
    if features['has_not_exists']:
        incompatible_features.append('NOT EXISTS subquery')
    if features['has_intersect']:
        incompatible_features.append('INTERSECT')
    if features['has_except']:
        incompatible_features.append('EXCEPT')
    if features['has_correlated_subquery']:
        incompatible_features.append('Correlated subquery')
    if features['has_subquery_from']:
        incompatible_features.append('Subquery in FROM clause')

    # Features that make implementation very complex but not impossible
    if features['union_count'] > 2:
        incompatible_features.append(f'Multiple UNIONs ({features["union_count"]})')

    is_compatible = len(incompatible_features) == 0
    return is_compatible, incompatible_features

def analyze_all_queries():
    """Analyze all TPC-DS queries for DataFrame API compatibility"""
    query_dir = "/workspace/benchmarks/tpcds_queries"
    results = {}

    compatible_queries = []
    incompatible_queries = []

    for i in range(1, 100):  # TPC-DS has 99 queries
        sql_file = os.path.join(query_dir, f"q{i}.sql")
        if os.path.exists(sql_file):
            with open(sql_file, 'r') as f:
                sql_content = f.read()

            features = analyze_sql_features(sql_content)
            is_compatible, issues = is_dataframe_compatible(features)

            results[i] = {
                'compatible': is_compatible,
                'issues': issues,
                'features': features
            }

            if is_compatible:
                compatible_queries.append(i)
            else:
                incompatible_queries.append(i)

    return results, compatible_queries, incompatible_queries

def main():
    print("Analyzing TPC-DS queries for DataFrame API compatibility...")
    print("=" * 80)

    results, compatible, incompatible = analyze_all_queries()

    print(f"\nSummary:")
    print(f"  Compatible queries: {len(compatible)} / 99")
    print(f"  Incompatible queries: {len(incompatible)} / 99")

    print(f"\n{'='*80}")
    print("COMPATIBLE QUERIES (can be implemented in pure DataFrame API):")
    print(f"{'='*80}")
    if compatible:
        print(f"Query numbers: {sorted(compatible)}")
        for q_num in sorted(compatible):
            print(f"  Q{q_num}: ✅ Compatible")
    else:
        print("  None - all queries have incompatible features")

    print(f"\n{'='*80}")
    print("INCOMPATIBLE QUERIES (require SQL features not available in DataFrame API):")
    print(f"{'='*80}")

    # Group by issue type
    issue_groups = {}
    for q_num in sorted(incompatible):
        issues = results[q_num]['issues']
        for issue in issues:
            if issue not in issue_groups:
                issue_groups[issue] = []
            issue_groups[issue].append(q_num)

    print("\nGrouped by incompatible feature:")
    for issue, queries in sorted(issue_groups.items()):
        print(f"\n  {issue}: {len(queries)} queries")
        print(f"    Queries: {queries[:10]}{'...' if len(queries) > 10 else ''}")

    # Detailed breakdown
    print(f"\n{'='*80}")
    print("DETAILED ANALYSIS:")
    print(f"{'='*80}")

    for q_num in range(1, 100):
        if q_num in results:
            result = results[q_num]
            status = "✅ COMPATIBLE" if result['compatible'] else "❌ INCOMPATIBLE"
            print(f"\nQuery {q_num}: {status}")
            if not result['compatible']:
                print(f"  Issues: {', '.join(result['issues'])}")

    # Save results to file
    output_file = "/workspace/tests/integration/tpcds_dataframe/dataframe_compatibility_analysis.txt"
    with open(output_file, 'w') as f:
        f.write("TPC-DS DataFrame API Compatibility Analysis\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Compatible queries ({len(compatible)}): {sorted(compatible)}\n")
        f.write(f"Incompatible queries ({len(incompatible)}): {sorted(incompatible)}\n\n")

        f.write("Detailed Analysis:\n")
        for q_num in range(1, 100):
            if q_num in results:
                result = results[q_num]
                status = "COMPATIBLE" if result['compatible'] else "INCOMPATIBLE"
                f.write(f"Q{q_num}: {status}")
                if not result['compatible']:
                    f.write(f" - {', '.join(result['issues'])}")
                f.write("\n")

    print(f"\nAnalysis saved to: {output_file}")

    return compatible, incompatible

if __name__ == "__main__":
    compatible, incompatible = main()