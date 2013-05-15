package com.facebook.presto.argus;

import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.PeregrineSqlParser;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRewriter;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.TreeRewriter;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

import java.util.List;

public final class QueryTranslator
{
    private QueryTranslator() {}

    public static String translateQuery(String sql)
    {
        Statement statement;
        try {
            statement = PeregrineSqlParser.createStatement(sql);
        }
        catch (ParsingException e) {
            return sql;
        }

        statement = TreeRewriter.rewriteWith(new Rewriter(), statement);

        return SqlFormatter.formatSql(statement);
    }

    private static class Rewriter
            extends NodeRewriter<Void>
    {
        @Override
        public Node rewriteFunctionCall(FunctionCall node, final Void context, final TreeRewriter<Void> treeRewriter)
        {
            List<Expression> args = FluentIterable.from(node.getArguments())
                    .transform(new Function<Expression, Expression>()
                    {
                        @Override
                        public Expression apply(Expression input)
                        {
                            return treeRewriter.rewrite(input, context);
                        }
                    })
                    .toList();

            String name = node.getName().toString().toLowerCase();

            if (name.equals("approx_count_distinct")) {
                return functionCall("approx_distinct", node);
            }
            if (name.equals("get_json_object")) {
                return functionCall("json_extract_scalar", node);
            }

            if (name.equals("cast_as_double")) {
                if (args.size() != 1) {
                    return treeRewriter.defaultRewrite(node, context);
                }
                return new Cast(args.get(0), "double");
            }
            if (name.equals("cast_as_bigint")) {
                if (args.size() != 1) {
                    return treeRewriter.defaultRewrite(node, context);
                }
                return new Cast(args.get(0), "bigint");
            }
            if (name.equals("cast_as_string")) {
                if (args.size() != 1) {
                    return treeRewriter.defaultRewrite(node, context);
                }
                return new Cast(args.get(0), "varchar");
            }

            if (name.equals("divide")) {
                if (args.size() != 2) {
                    return treeRewriter.defaultRewrite(node, context);
                }
                return new ArithmeticExpression(ArithmeticExpression.Type.DIVIDE, args.get(0), args.get(1));
            }

            if (name.equals("equal_to")) {
                if (args.size() != 2) {
                    return treeRewriter.defaultRewrite(node, context);
                }
                return new ComparisonExpression(ComparisonExpression.Type.EQUAL, args.get(0), args.get(1));
            }
            if (name.equals("greater_than")) {
                if (args.size() != 2) {
                    return treeRewriter.defaultRewrite(node, context);
                }
                return new ComparisonExpression(ComparisonExpression.Type.GREATER_THAN, args.get(0), args.get(1));
            }

            if (name.equals("concat")) {
                if (args.size() < 2) {
                    return treeRewriter.defaultRewrite(node, context);
                }
                return rewriteConcat(args);
            }

            return treeRewriter.defaultRewrite(node, context);
        }

        private static FunctionCall functionCall(String name, FunctionCall node)
        {
            return new FunctionCall(QualifiedName.of(name), node.getWindow().orNull(), node.isDistinct(), node.getArguments());
        }
    }

    private static Node rewriteConcat(List<Expression> args)
    {
        FunctionCall node = new FunctionCall(QualifiedName.of("concat"), null, false, args.subList(0, 2));
        args = args.subList(2, args.size());
        while (!args.isEmpty()) {
            node = new FunctionCall(QualifiedName.of("concat"), null, false, ImmutableList.of(node, args.get(0)));
            args = args.subList(1, args.size());
        }
        return node;
    }
}
