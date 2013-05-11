package com.facebook.presto.sql.parser;

import com.facebook.presto.sql.tree.Statement;
import com.google.common.annotations.VisibleForTesting;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenStream;
import org.antlr.runtime.tree.BufferedTreeNodeStream;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.TreeNodeStream;

public final class PeregrineSqlParser
{
    private PeregrineSqlParser() {}

    public static Statement createStatement(String sql)
    {
        return createStatement(parseStatement(sql));
    }

    @VisibleForTesting
    static Statement createStatement(CommonTree tree)
    {
        TreeNodeStream stream = new BufferedTreeNodeStream(tree);
        PeregrineStatementBuilder builder = new PeregrineStatementBuilder(stream);
        try {
            return builder.statement().value;
        }
        catch (RecognitionException e) {
            throw new AssertionError(e); // RecognitionException is not thrown
        }
    }

    @VisibleForTesting
    static CommonTree parseStatement(String sql)
    {
        try {
            return (CommonTree) getParser(sql).singleStatement().getTree();
        }
        catch (RecognitionException e) {
            throw new AssertionError(e); // RecognitionException is not thrown
        }
    }

    private static PeregrineStatementParser getParser(String sql)
    {
        CharStream stream = new CaseInsensitiveStream(new ANTLRStringStream(sql));
        PeregrineStatementLexer lexer = new PeregrineStatementLexer(stream);
        TokenStream tokenStream = new CommonTokenStream(lexer);
        return new PeregrineStatementParser(tokenStream);
    }
}
