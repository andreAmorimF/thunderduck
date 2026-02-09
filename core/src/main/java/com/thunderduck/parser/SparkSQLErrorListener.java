package com.thunderduck.parser;

import org.antlr.v4.runtime.*;

/**
 * Custom ANTLR error listener that produces user-friendly error messages
 * for SparkSQL parse failures.
 *
 * <p>Converts ANTLR4 error events into {@link SparkSQLParseException} with:
 * <ul>
 *   <li>Line and column position within the original SQL</li>
 *   <li>The offending token text</li>
 *   <li>Description of expected alternatives</li>
 * </ul>
 */
public class SparkSQLErrorListener extends BaseErrorListener {

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer,
                           Object offendingSymbol,
                           int line,
                           int charPositionInLine,
                           String msg,
                           RecognitionException e) {
        String tokenText = "";
        if (offendingSymbol instanceof Token token) {
            tokenText = token.getText();
        }

        throw new SparkSQLParseException(line, charPositionInLine, tokenText,
            "Syntax error at line %d:%d near '%s': %s".formatted(
                line, charPositionInLine, tokenText, msg));
    }
}
