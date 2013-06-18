package com.facebook.presto.argus;

import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.parser.PeregrineSqlParser;
import com.facebook.presto.sql.tree.AliasedExpression;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRewriter;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.TreeRewriter;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

public final class QueryTranslator
{
    private static final StringLiteral DEFAULT_DATE_FORMAT = new StringLiteral("%Y-%m-%d %H:%i:%s");

    private QueryTranslator() {}

    public static String translateQuery(String sql)
    {
        sql = sql.replaceAll("(?)percentile_array\\[(\\d+)]",
                "cast(json_extract_scalar(percentile_array, '\\$[$1]') as double)");

        try {
            Statement statement = PeregrineSqlParser.createStatement(sql);
            statement = TreeRewriter.rewriteWith(new Rewriter(), statement);
            return SqlFormatter.formatSql(statement);
        }
        catch (RuntimeException e) {
            System.err.println(e);
            return sql;
        }
    }

    private static class Rewriter
            extends NodeRewriter<Void>
    {
        @SuppressWarnings("AssignmentToForLoopParameter")
        @Override
        public Node rewriteQuery(Query node, Void context, TreeRewriter<Void> treeRewriter)
        {
            node = treeRewriter.defaultRewrite(node, context);

            Map<String, Integer> aliases = extractAliases(node);

            ImmutableList.Builder<Expression> groupBy = ImmutableList.builder();
            for (Expression expression : node.getGroupBy()) {
                if (expression instanceof QualifiedNameReference) {
                    String name = ((QualifiedNameReference) expression).getName().toString().toLowerCase();
                    if (aliases.containsKey(name)) {
                        expression = new LongLiteral(aliases.get(name).toString());
                    }
                }
                groupBy.add(expression);
            }

            return new Query(
                    node.getWith(),
                    node.getSelect(),
                    node.getFrom(),
                    node.getWhere(),
                    groupBy.build(),
                    node.getHaving(),
                    node.getOrderBy(),
                    node.getLimit(),
                    node.getUnion());
        }

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

            if (name.equals("count")) {
                if ((args.size() == 1) && (args.get(0) instanceof ComparisonExpression)) {
                    return functionCall("count_if", node, args);
                }
            }

            if (name.equals("array_get")) {
                String path = "$[" + ((LongLiteral) args.get(1)).getValue() +  "]";
                args = ImmutableList.of(args.get(0), new StringLiteral(path));
                return functionCall("json_extract_scalar", node, args);
            }

            if (name.equals("find_in_array")) {
                if (args.size() != 2) {
                    return treeRewriter.defaultRewrite(node, context);
                }
                return functionCall("json_array_contains", node, ImmutableList.of(args.get(1), args.get(0)));
            }

            if (name.equals("size")) {
                return functionCall("json_array_length", node, args);
            }

            if (name.equals("approx_percentile")) {
                return new Cast(node, "double");
            }

            if (name.equals("approx_count_distinct")) {
                return functionCall("approx_distinct", node, args);
            }
            if (name.equals("get_json_object")) {
                return functionCall("json_extract_scalar", node, args);
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

            if (name.equals("strftime")) {
                if (args.size() == 1) {
                    args = ImmutableList.of(args.get(0), DEFAULT_DATE_FORMAT);
                }
                else if ((args.size() == 2) && (args.get(1) instanceof StringLiteral)) {
                    String format = ((StringLiteral) args.get(1)).getValue();
                    format = format.replace("%M", "%i");
                    args = ImmutableList.of(args.get(0), new StringLiteral(format));
                }
                return functionCall("date_format", node, args);
            }
            if (name.equals("strptime")) {
                if (args.size() == 1) {
                    args = ImmutableList.of(args.get(0), DEFAULT_DATE_FORMAT);
                }
                return functionCall("date_parse", node, args);
            }

            if (name.equals("rlike")) {
                if ((args.size() == 2) && (args.get(1) instanceof StringLiteral)) {
                    String regex = ((StringLiteral) args.get(1)).getValue();
                    if ((!regex.startsWith(".*")) && (!regex.startsWith("^"))) {
                        regex = "^" + regex;
                    }
                    if ((!regex.endsWith(".*")) && (!regex.endsWith("$"))) {
                        regex += "$";
                    }
                    args = ImmutableList.of(args.get(0), new StringLiteral(regex));
                    return functionCall("regexp_like", node, args);
                }
                return treeRewriter.defaultRewrite(node, context);
            }

            return treeRewriter.defaultRewrite(node, context);
        }

        @Override
        public Node rewriteArithmeticExpression(ArithmeticExpression node, Void context, TreeRewriter<Void> treeRewriter)
        {
            if (node.getType() == ArithmeticExpression.Type.ADD) {
                Expression left = treeRewriter.rewrite(node.getLeft(), context);
                Expression right = treeRewriter.rewrite(node.getRight(), context);
                if (isStringType(left) || isStringType(right)) {
                    return rewriteConcat(ImmutableList.of(left, right));
                }
            }

            return treeRewriter.defaultRewrite(node, context);
        }

        @Override
        public Node rewriteComparisonExpression(ComparisonExpression node, Void context, TreeRewriter<Void> treeRewriter)
        {
            if (node.getType() == ComparisonExpression.Type.GREATER_THAN) {
                Expression left = treeRewriter.rewrite(node.getLeft(), context);
                Expression right = treeRewriter.rewrite(node.getRight(), context);
                if (left instanceof FunctionCall) {
                    String name = ((FunctionCall) left).getName().toString().toLowerCase();
                    if (name.equals("json_array_contains")) {
                        if ((right instanceof LongLiteral) && (((LongLiteral) right).getValue() == 0)) {
                            return left;
                        }
                    }
                }
            }

            return treeRewriter.defaultRewrite(node, context);
        }
    }

    private static boolean isStringType(Expression e)
    {
        return (e instanceof StringLiteral) || isFunction(e, "concat");
    }

    private static boolean isFunction(Expression e, String name)
    {
        return (e instanceof FunctionCall) && (((FunctionCall) e).getName().toString().equalsIgnoreCase(name));
    }

    private static FunctionCall functionCall(String name, FunctionCall node, List<Expression> args)
    {
        return new FunctionCall(QualifiedName.of(name), node.getWindow().orNull(), node.isDistinct(), args);
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

    private static Map<String, Integer> extractAliases(Query query)
    {
        ImmutableMap.Builder<String, Integer> aliases = ImmutableMap.builder();
        int ordinal = 1;
        for (Expression expression : query.getSelect().getSelectItems()) {
            if (expression instanceof AliasedExpression) {
                AliasedExpression alias = (AliasedExpression) expression;
                aliases.put(alias.getAlias().toLowerCase(), ordinal);
            }
            ordinal++;
        }
        return aliases.build();
    }
}
