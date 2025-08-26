package com.example.parser;

import java.util.Locale;

/**
 * @author yujin
 * @date 2025/8/25 14:49
 **/
public class SqlPreprocessor {
    private final boolean enableHiveNormalization;

    public SqlPreprocessor(boolean enableHiveNormalization) {
        this.enableHiveNormalization = enableHiveNormalization;
    }

    public String preprocess(String sql) {
        if (!enableHiveNormalization) {
            return sql;
        }
        String s = removeComments(sql);
        s = stripHints(s);
        s = normalizeInsertOverwritePartition(s);
        s = stripHiveOrderClauses(s); // DISTRIBUTE/SORT/CLUSTER BY
        return s;
    }

    private String removeComments(String s) {
        StringBuilder out = new StringBuilder();
        boolean inSL=false, inML=false, inSQ=false, inDQ=false;
        for (int i=0;i<s.length();i++) {
            char c=s.charAt(i), n=(i+1<s.length()?s.charAt(i+1):'\0');
            if (!inSQ && !inDQ && !inML && c=='-' && n=='-') { inSL=true; i++; continue; }
            if (inSL && (c=='\n' || c=='\r')) { inSL=false; out.append(c); continue; }
            if (inSL) {
                continue;
            }
            if (!inSQ && !inDQ && !inSL && c=='/' && n=='*') { inML=true; i++; continue; }
            if (inML && c=='*' && n=='/') { inML=false; i++; continue; }
            if (inML) {
                continue;
            }
            if (c=='\'') {
                inSQ = !inSQ;
            } else if (c=='"') {
                inDQ = !inDQ;
            }
            out.append(c);
        }
        return out.toString();
    }

    private String stripHints(String s) {
        StringBuilder out = new StringBuilder();
        for (int i=0; i<s.length(); i++) {
            if (i+2 < s.length() && s.charAt(i)=='/' && s.charAt(i+1)=='*' && s.charAt(i+2)=='+') {
                i += 3; int depth = 1;
                while (i < s.length() && depth > 0) {
                    if (i+1 < s.length() && s.charAt(i)=='*' && s.charAt(i+1)=='/') { depth--; i+=2; break; }
                    i++;
                }
                continue;
            }
            out.append(s.charAt(i));
        }
        return out.toString();
    }

    // INSERT OVERWRITE TABLE <t> [PARTITION(...)] SELECT ...  => INSERT INTO <t> SELECT ...
    private String normalizeInsertOverwritePartition(String s) {
        String lowerAll = s.toLowerCase(Locale.ROOT);
        String token = "insert overwrite table";
        int idx = lowerAll.indexOf(token);
        if (idx < 0) {
            return s;
        }

        StringBuilder out = new StringBuilder();
        int i=0;
        while (i < s.length()) {
            int hit = s.toLowerCase(Locale.ROOT).indexOf(token, i);
            if (hit < 0) { out.append(s.substring(i)); break; }
            out.append(s, i, hit);
            out.append("INSERT INTO ");
            i = hit + token.length();
            while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
                i++;
            }
            // 读取表名（支持 `db`.`t` / db.t）
            int start = i; boolean inQuote=false;
            while (i < s.length()) {
                char c = s.charAt(i);
                if (!inQuote && (Character.isWhitespace(c) || c=='(')) {
                    break;
                }
                if (c=='\'' || c=='"' || c=='`') {
                    inQuote = !inQuote;
                }
                i++;
            }
            String table = s.substring(start, i)
                    .replace("`.", ".").replace(".`", ".")
                    .replace("\".", ".").replace(".\"", ".");
            out.append(table).append(' ');
            // 跳过紧随其后的 PARTITION(...)
            int save = i;

            while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
                i++;
            }
            String tailLower = s.substring(i).toLowerCase(Locale.ROOT);
            if (tailLower.startsWith("partition")) {
                i += "partition".length();
                while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
                    i++;
                }
                if (i < s.length() && s.charAt(i)=='(') {
                    int depth=1; i++;
                    while (i < s.length() && depth>0) {
                        char c = s.charAt(i++);
                        if (c=='(') {
                            depth++;
                        } else if (c==')') {
                            depth--;
                        }
                    }
                } else { i = save; }
            } else { i = save; }
        }
        return out.toString();
    }

    private String stripHiveOrderClauses(String s) {
        String[] keys = {"distribute by", "sort by", "cluster by"};
        String lower = s.toLowerCase(Locale.ROOT);
        for (String k : keys) {
            int idx;
            while ((idx = lower.indexOf(k)) >= 0) {
                int i = idx;
                while (i < s.length() && s.charAt(i) != ';' && s.charAt(i) != '\n') {
                    i++;
                }
                s = s.substring(0, idx) + s.substring(i);
                lower = s.toLowerCase(Locale.ROOT);
            }
        }
        return s;
    }
}
