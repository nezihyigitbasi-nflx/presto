package com.facebook.presto.argus;

import com.facebook.presto.argus.peregrine.QueryResult;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.parseDouble;
import static java.lang.Long.parseLong;
import static java.util.Collections.unmodifiableList;

public final class PeregrineUtil
{
    private PeregrineUtil() {}

    public static List<List<Object>> peregrineResults(QueryResult result)
    {
        List<Function<String, Object>> types = new ArrayList<>();
        for (String type : result.getTypes()) {
            types.add(typeConversionFunction(type));
        }

        Splitter splitter = Splitter.on((char) 1);
        ImmutableList.Builder<List<Object>> rows = ImmutableList.builder();

        for (String line : result.getValues()) {
            List<String> values = ImmutableList.copyOf(splitter.split(line).iterator());
            checkArgument(values.size() == types.size(), "wrong column count (expected %s, was %s)", types.size(), values.size());

            List<Object> row = new ArrayList<>();
            for (int i = 0; i < types.size(); i++) {
                row.add(types.get(i).apply(values.get(i)));
            }
            rows.add(unmodifiableList(row));
        }

        return rows.build();
    }

    private static Function<String, Object> typeConversionFunction(String type)
    {
        switch (type) {
            case "string":
                return new Function<String, Object>()
                {
                    @Override
                    public Object apply(String s)
                    {
                        return s;
                    }
                };
            case "bigint":
                return new Function<String, Object>()
                {
                    @Override
                    public Object apply(String s)
                    {
                        return s.equals("null") ? null : parseLong(s);
                    }
                };
            case "double":
                return new Function<String, Object>()
                {
                    @Override
                    public Object apply(String s)
                    {
                        return s.equals("null") ? null : parseDouble(s);
                    }
                };
        }
        throw new IllegalArgumentException("Unsupported Peregrine type: " + type);
    }
}
