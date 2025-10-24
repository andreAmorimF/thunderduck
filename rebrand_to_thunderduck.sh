#!/bin/bash
# Script to rebrand catalyst2sql to thunderduck
# This script performs case-preserving replacements

set -e

echo "=== Rebranding catalyst2sql to Thunderduck ==="
echo ""

# Function to perform replacements
perform_replacements() {
    local file="$1"

    # Skip this script itself
    if [[ "$file" == *"rebrand_to_thunderduck.sh"* ]]; then
        return
    fi

    # Skip .git directory
    if [[ "$file" == *".git/"* ]]; then
        return
    fi

    # Skip binary files and common non-text files
    if [[ "$file" == *.jar ]] || [[ "$file" == *.class ]] || [[ "$file" == *.png ]] || [[ "$file" == *.jpg ]] || [[ "$file" == *.gif ]]; then
        return
    fi

    # Perform replacements with sed (case-sensitive)
    # 1. catalyst2sql -> thunderduck
    # 2. Catalyst2SQL -> Thunderduck (preserving initial caps)
    # 3. CATALYST2SQL -> THUNDERDUCK (all caps)

    sed -i 's/catalyst2sql/thunderduck/g' "$file" 2>/dev/null || true
    sed -i 's/Catalyst2SQL/Thunderduck/g' "$file" 2>/dev/null || true
    sed -i 's/CATALYST2SQL/THUNDERDUCK/g' "$file" 2>/dev/null || true
}

export -f perform_replacements

# Find all relevant files and apply replacements
echo "Step 1: Replacing text in source files..."
find . -type f \( \
    -name "*.java" -o \
    -name "*.xml" -o \
    -name "*.properties" -o \
    -name "*.md" -o \
    -name "*.txt" -o \
    -name "*.sh" -o \
    -name "*.yml" -o \
    -name "*.yaml" \
\) -exec bash -c 'perform_replacements "$0"' {} \;

echo "✓ Text replacements complete"
echo ""

# Step 2: Rename directory structure
echo "Step 2: Renaming directory structure..."

# Find all com/catalyst2sql directories and rename to com/thunderduck
find . -type d -path "*/com/catalyst2sql" | while read -r dir; do
    new_dir="${dir/catalyst2sql/thunderduck}"
    if [ "$dir" != "$new_dir" ]; then
        echo "  Renaming: $dir -> $new_dir"
        mv "$dir" "$new_dir"
    fi
done

echo "✓ Directory structure renamed"
echo ""

# Step 3: Show summary
echo "=== Rebranding Summary ==="
echo "Replaced the following patterns:"
echo "  • catalyst2sql → thunderduck"
echo "  • Catalyst2SQL → Thunderduck"
echo "  • CATALYST2SQL → THUNDERDUCK"
echo ""
echo "Directory structure updated:"
echo "  • com/catalyst2sql → com/thunderduck"
echo ""
echo "✓ Rebranding complete!"
echo ""
echo "Next steps:"
echo "  1. Review changes: git status"
echo "  2. Check diffs: git diff"
echo "  3. Test build: mvn clean compile"
echo "  4. Commit: git add . && git commit -m 'Rebrand catalyst2sql to Thunderduck'"
