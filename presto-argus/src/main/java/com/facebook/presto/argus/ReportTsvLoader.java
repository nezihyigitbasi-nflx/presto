package com.facebook.presto.argus;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.airlift.json.JsonCodec;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Strings.isNullOrEmpty;

public final class ReportTsvLoader
{
    public static final JsonCodec<Map<String, Object>> CODEC = JsonCodec.mapJsonCodec(String.class, Object.class);

    private ReportTsvLoader() {}

    @SuppressWarnings("AssignmentToForLoopParameter")
    public static List<Report> loadReports()
            throws IOException
    {
        File file = new File("/Users/dphillips/tmp/argus.tsv");
        String data = Files.toString(file, Charsets.US_ASCII);
        data = data.replace("\r", "");

        ImmutableList.Builder<Report> reports = ImmutableList.builder();
        for (String line : Splitter.on('\n').split(data)) {
            if (line.isEmpty()) {
                continue;
            }
            line = line.replace("\\n", "\n");
            Iterator<String> iter = Splitter.on('\t').split(line).iterator();
            long reportId = Long.parseLong(iter.next());
            String namespace = iter.next();
            String query = unescape(iter.next());
            Map<String, String> variables = parseVariables(unescape(iter.next()));
            long views = Long.parseLong(iter.next());
            reports.add(new Report(reportId, namespace, query, variables, views));
        }
        return reports.build();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> parseVariables(String json)
    {
        if (isNullOrEmpty(json) || "null".equals(json) || "[]".equals(json)) {
            return ImmutableMap.of();
        }
        Map<String, Object> map = CODEC.fromJson(json);
        ImmutableMap.Builder<String, String> variables = ImmutableMap.builder();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getValue() instanceof Map) {
                Map<String, Object> valueMap = (Map<String, Object>) entry.getValue();
                if (valueMap.containsKey("default")) {
                    Object value = valueMap.get("default");
                    if (value instanceof String) {
                        variables.put(entry.getKey(), (String) value);
                    }
                }
            }
        }
        return variables.build();
    }

    private static String unescape(String s)
    {
        s = s.replace("\\t", "\t").replace("\\\\", "\\");
        return s.equals("NULL") ? null : s;
    }
}
