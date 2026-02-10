package com.thunderduck.parser;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.misc.Interval;

/**
 * A CharStream wrapper that makes the ANTLR lexer case-insensitive.
 *
 * <p>The lexer sees uppercase characters (so keyword rules like {@code AS: 'AS';}
 * match regardless of input casing), while {@link #getText(Interval)} returns
 * the original text (preserving user casing for identifiers and literals).
 *
 * <p>This matches the approach used by Apache Spark's SQL parser.
 */
public final class UpperCaseCharStream implements CharStream {

    private final CharStream wrapped;

    public UpperCaseCharStream(CharStream wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public String getText(Interval interval) {
        // Return original text (preserves user casing)
        return wrapped.getText(interval);
    }

    @Override
    public void consume() {
        wrapped.consume();
    }

    @Override
    public int LA(int i) {
        int c = wrapped.LA(i);
        if (c <= 0) {
            return c;
        }
        return Character.toUpperCase(c);
    }

    @Override
    public int mark() {
        return wrapped.mark();
    }

    @Override
    public void release(int marker) {
        wrapped.release(marker);
    }

    @Override
    public int index() {
        return wrapped.index();
    }

    @Override
    public void seek(int index) {
        wrapped.seek(index);
    }

    @Override
    public int size() {
        return wrapped.size();
    }

    @Override
    public String getSourceName() {
        return wrapped.getSourceName();
    }
}
