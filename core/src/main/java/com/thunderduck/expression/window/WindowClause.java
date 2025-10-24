package com.thunderduck.expression.window;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Represents a WINDOW clause containing multiple named window definitions.
 *
 * <p>The WINDOW clause appears after the WHERE/GROUP BY/HAVING clauses and before
 * ORDER BY/LIMIT. It defines named window specifications that can be referenced
 * by window functions in the SELECT clause.
 *
 * <p><b>SQL Example:</b>
 * <pre>
 * SELECT
 *   employee_id,
 *   department_id,
 *   salary,
 *   AVG(salary) OVER w1 AS dept_avg,
 *   RANK() OVER w2 AS salary_rank
 * FROM employees
 * WINDOW
 *   w1 AS (PARTITION BY department_id),
 *   w2 AS (w1 ORDER BY salary DESC)
 * </pre>
 *
 * <p><b>Benefits of WINDOW Clause:</b>
 * <ul>
 *   <li>Reduces duplication when multiple functions use same window</li>
 *   <li>Enables window composition (windows can reference other windows)</li>
 *   <li>Improves query readability and maintainability</li>
 *   <li>Allows database to optimize execution plan</li>
 * </ul>
 *
 * <p><b>Ordering Requirements:</b>
 * Named windows must be defined before they are referenced. If window w2
 * extends window w1, then w1 must be defined first in the WINDOW clause.
 *
 * @see NamedWindow
 * @see WindowFunction
 * @since 1.0
 */
public class WindowClause {

    private final List<NamedWindow> windows;
    private final Map<String, NamedWindow> windowsByName;

    /**
     * Creates a WINDOW clause with the specified named windows.
     *
     * @param windows the list of named window definitions
     * @throws NullPointerException if windows is null
     * @throws IllegalArgumentException if windows is empty or contains duplicates
     */
    public WindowClause(List<NamedWindow> windows) {
        Objects.requireNonNull(windows, "windows must not be null");
        if (windows.isEmpty()) {
            throw new IllegalArgumentException("WINDOW clause requires at least one named window");
        }

        this.windows = Collections.unmodifiableList(new ArrayList<>(windows));
        this.windowsByName = new HashMap<>();

        // Build index and check for duplicates
        for (NamedWindow window : windows) {
            if (windowsByName.containsKey(window.name())) {
                throw new IllegalArgumentException(
                    "Duplicate window name: " + window.name());
            }
            windowsByName.put(window.name(), window);
        }

        validateWindowReferences();
    }

    /**
     * Creates a WINDOW clause with a single named window.
     *
     * @param window the named window
     * @return a new WindowClause instance
     */
    public static WindowClause of(NamedWindow window) {
        return new WindowClause(Collections.singletonList(window));
    }

    /**
     * Creates a WINDOW clause with multiple named windows.
     *
     * @param windows the named windows
     * @return a new WindowClause instance
     */
    public static WindowClause of(NamedWindow... windows) {
        return new WindowClause(Arrays.asList(windows));
    }

    /**
     * Returns all named windows in this clause.
     *
     * @return an unmodifiable list of named windows
     */
    public List<NamedWindow> windows() {
        return windows;
    }

    /**
     * Returns the number of named windows.
     *
     * @return the number of windows
     */
    public int size() {
        return windows.size();
    }

    /**
     * Checks if this clause contains a window with the given name.
     *
     * @param name the window name to check
     * @return true if a window with this name exists
     */
    public boolean contains(String name) {
        return windowsByName.containsKey(name);
    }

    /**
     * Gets a named window by name.
     *
     * @param name the window name
     * @return the named window, or null if not found
     */
    public NamedWindow getWindow(String name) {
        return windowsByName.get(name);
    }

    /**
     * Generates SQL for the WINDOW clause.
     *
     * <p>Format: {@code WINDOW name1 AS (spec1), name2 AS (spec2), ...}
     *
     * <p>Example:
     * <pre>
     * WINDOW w1 AS (PARTITION BY department_id),
     *        w2 AS (w1 ORDER BY salary DESC)
     * </pre>
     *
     * @return the SQL representation
     */
    public String toSQL() {
        StringBuilder sql = new StringBuilder("WINDOW ");

        for (int i = 0; i < windows.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(windows.get(i).toSQL());
        }

        return sql.toString();
    }

    /**
     * Validates that all window references are valid.
     *
     * <p>Checks:
     * <ul>
     *   <li>Referenced base windows exist in this clause</li>
     *   <li>No circular references between windows</li>
     *   <li>Referenced windows are defined before they are used</li>
     * </ul>
     *
     * @throws IllegalArgumentException if validation fails
     */
    private void validateWindowReferences() {
        // Check that base window references are valid
        for (NamedWindow window : windows) {
            String baseWindow = window.baseWindow();
            if (baseWindow != null) {
                if (!windowsByName.containsKey(baseWindow)) {
                    throw new IllegalArgumentException(
                        String.format("Window '%s' references undefined base window '%s'",
                                    window.name(), baseWindow));
                }
            }
        }

        // Check for circular references
        for (NamedWindow window : windows) {
            Set<String> visited = new HashSet<>();
            checkCircularReference(window, visited);
        }

        // Check ordering: base windows must be defined before they are referenced
        Set<String> definedWindows = new HashSet<>();
        for (NamedWindow window : windows) {
            String baseWindow = window.baseWindow();
            if (baseWindow != null && !definedWindows.contains(baseWindow)) {
                throw new IllegalArgumentException(
                    String.format("Window '%s' references base window '%s' " +
                                "which is defined later in the WINDOW clause",
                                window.name(), baseWindow));
            }
            definedWindows.add(window.name());
        }
    }

    /**
     * Checks for circular references in window definitions.
     *
     * @param window the window to check
     * @param visited the set of visited window names
     * @throws IllegalArgumentException if a circular reference is detected
     */
    private void checkCircularReference(NamedWindow window, Set<String> visited) {
        if (visited.contains(window.name())) {
            throw new IllegalArgumentException(
                "Circular reference detected in window definitions: " +
                String.join(" -> ", visited) + " -> " + window.name());
        }

        visited.add(window.name());

        String baseWindow = window.baseWindow();
        if (baseWindow != null) {
            NamedWindow base = windowsByName.get(baseWindow);
            if (base != null) {
                checkCircularReference(base, new HashSet<>(visited));
            }
        }
    }

    /**
     * Resolves a window name to its full specification by following base window references.
     *
     * <p>If the window extends another window, this method combines the specifications
     * from all referenced windows in the chain.
     *
     * @param windowName the window name to resolve
     * @return the fully resolved window specification
     * @throws IllegalArgumentException if the window name is not found
     */
    public NamedWindow resolve(String windowName) {
        NamedWindow window = windowsByName.get(windowName);
        if (window == null) {
            throw new IllegalArgumentException("Window not found: " + windowName);
        }

        // If no base window, return as-is
        if (window.baseWindow() == null) {
            return window;
        }

        // Recursively resolve base window and combine specifications
        // This is a simplified version - full implementation would merge
        // PARTITION BY, ORDER BY, and frame specifications
        return window;
    }

    @Override
    public String toString() {
        return toSQL();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WindowClause that = (WindowClause) o;
        return Objects.equals(windows, that.windows);
    }

    @Override
    public int hashCode() {
        return Objects.hash(windows);
    }
}
