package com.example.parser;

import java.util.*;

public class TokenScannerFallback {

    /** 兜底解析：返回【多个目标】与【来源表集合】 */
    public JSqlParserEngine.FallbackResult extract(String sql) {
        if (sql == null) {
            return new JSqlParserEngine.FallbackResult(Collections.<String>emptySet(), Collections.<String>emptySet());
        }
        String s = sql;
        String lower = s.toLowerCase(Locale.ROOT);

        // 先收集 CTE 名（WITH a AS (...), b AS (...)）
        Set<String> cteNames = collectCteNames(s);

        // 识别三类目标：CREATE VIEW、INSERT ALL/FIRST、多 INSERT（Hive）、普通 INSERT INTO/OVERWRITE
        Set<String> targets = new LinkedHashSet<>();

        int posCreate = indexOfWord(lower, "create", 0);
        if (posCreate >= 0 && startsWithWord(lower, posCreate, "create")) {
            int posView = indexOfWord(lower, "view", posCreate + 6);
            if (posView >= 0 && posView - posCreate < 20) {
                // CREATE [MATERIALIZED] VIEW <ident> AS SELECT ...
                int p = posView + 4;
                while (p < s.length() && Character.isWhitespace(s.charAt(p))) { p++; }
                String view = readIdentifier(s, p);
                if (view != null) {
                    targets.add(view);
                }
            }
        }

        int posInsert = indexOfWord(lower, "insert", 0);
        boolean isInsertAll = false;
        if (posInsert >= 0) {
            if (indexOfWord(lower, "insert all", posInsert) == posInsert ||
                    indexOfWord(lower, "insert first", posInsert) == posInsert) {
                isInsertAll = true;
            }
        }

        if (isInsertAll) {
            // Oracle INSERT ALL/FIRST：在第一条 SELECT 之前收集所有 INTO <table>
            int posSelect = indexOfWord(lower, "select", posInsert);
            int scanEnd = (posSelect >= 0) ? posSelect : s.length();
            int i = posInsert;
            while (true) {
                int into = indexOfWord(lower, "into", i);
                if (into < 0 || into >= scanEnd) { break; }
                int p = into + 4;
                while (p < scanEnd && Character.isWhitespace(s.charAt(p))) { p++; }
                String ident = readIdentifier(s, p);
                if (ident != null && !ident.isEmpty()) {
                    targets.add(ident);
                }
                i = p + 1;
            }
        }

        // Hive 多目标写法：FROM (...) [alias] INSERT OVERWRITE/INTO TABLE t1 SELECT ...
        // 以及普通 INSERT INTO/OVERWRITE
        // 扫描所有 INSERT ... 片段，抽取每个目标
        int scanPos = 0;
        while (true) {
            int nextIns = indexOfWord(lower, "insert", scanPos);
            if (nextIns < 0) { break; }
            String tgt = findSingleTarget(s, lower, nextIns);
            if (tgt != null) {
                targets.add(tgt);
            }
            scanPos = nextIns + 6;
        }

        // 来源表：两路合并
        // A) 如果是 Hive 多目标写法：先从第一个 FROM（且位于首个 INSERT 之前）中提取来源
        Set<String> sources = new LinkedHashSet<>();
        if (posInsert >= 0) {
            int posFromBeforeInsert = indexOfWord(lower, "from", 0);
            if (posFromBeforeInsert >= 0 && posFromBeforeInsert < posInsert) {
                // 截出 FROM ... 到首个 INSERT 之间的段落，递归提取来源
                String head = s.substring(posFromBeforeInsert, posInsert);
                sources.addAll(extractSourcesRecursive(head, cteNames));
            }
        }

        // B) 再从所有 SELECT 段里提取（适配普通 INSERT/CTAS/VIEW）
        int posSelect = indexOfWord(lower, "select", 0);
        if (posSelect >= 0) {
            String tail = s.substring(posSelect);
            sources.addAll(extractSourcesRecursive(tail, cteNames));
        } else {
            // 没有显式 SELECT（极少见），那就全量扫一次
            sources.addAll(extractSourcesRecursive(s, cteNames));
        }

        return new JSqlParserEngine.FallbackResult(targets, sources);
    }

    /* -------------------- 识别 CREATE/INSERT 目标辅助 -------------------- */

    private static String findSingleTarget(String s, String lower, int posInsert) {
        if (posInsert < 0) {
            return null;
        }
        int p = posInsert + "insert".length();
        while (p < s.length() && Character.isWhitespace(s.charAt(p))) { p++; }

        if (startsWithWord(lower, p, "into")) {
            p += 4;
            while (p < s.length() && Character.isWhitespace(s.charAt(p))) { p++; }
            if (startsWithWord(lower, p, "table")) {
                p += 5;
                while (p < s.length() && Character.isWhitespace(s.charAt(p))) { p++; }
            }
            return readIdentifier(s, p);
        }

        if (startsWithWord(lower, p, "overwrite")) { // INSERT OVERWRITE TABLE <t>
            p += 9;
            while (p < s.length() && Character.isWhitespace(s.charAt(p))) { p++; }
            if (startsWithWord(lower, p, "table")) {
                p += 5;
                while (p < s.length() && Character.isWhitespace(s.charAt(p))) { p++; }
            }
            return readIdentifier(s, p);
        }

        return null;
    }

    /* -------------------- 来源表抽取（递归 FROM/JOIN + 逗号表列） -------------------- */

    private static List<String> extractSourcesRecursive(String s, Set<String> cteNames) {
        List<String> result = new ArrayList<>();
        String lower = s.toLowerCase(Locale.ROOT);
        int n = s.length();
        int i = 0;

        while (true) {
            int posFrom = indexOfWord(lower, "from", i);
            int posJoin = indexOfWord(lower, "join", i);
            int hit = minPos(posFrom, posJoin);
            if (hit < 0) {
                break;
            }

            boolean isFrom = (hit == posFrom);
            int p = hit + 4; // len("from"/"join")
            while (p < n && Character.isWhitespace(s.charAt(p))) { p++; }

            if (p < n && s.charAt(p) == '(') {
                // 子查询：跳过配对括号，递归解析内部
                int depth = 1;
                p++;
                int startSub = p;
                while (p < n && depth > 0) {
                    char c = s.charAt(p++);
                    if (c == '(') { depth++; }
                    else if (c == ')') { depth--; }
                }
                List<String> sub = extractSourcesRecursive(s.substring(startSub, p - 1), cteNames);
                for (String t : sub) {
                    if (!result.contains(t)) {
                        result.add(t);
                    }
                }
            } else {
                // 第一个表
                int[] endPos = new int[]{p};
                String ident = readIdentifier(s, p, endPos);
                if (ident != null && !cteNames.contains(ident) && !result.contains(ident)) {
                    result.add(ident);
                }

                // FROM 才继续吃逗号表列
                if (isFrom) {
                    int q = endPos[0];
                    while (q < n) {
                        int prev = q;
                        while (q < n && Character.isWhitespace(s.charAt(q))) { q++; }
                        if (q < n && s.charAt(q) == ',') {
                            q++;
                            while (q < n && Character.isWhitespace(s.charAt(q))) { q++; }
                            if (q < n && s.charAt(q) == '(') {
                                int depth = 1;
                                q++;
                                int startSub2 = q;
                                while (q < n && depth > 0) {
                                    char c = s.charAt(q++);
                                    if (c == '(') { depth++; }
                                    else if (c == ')') { depth--; }
                                }
                                List<String> sub2 = extractSourcesRecursive(s.substring(startSub2, q - 1), cteNames);
                                for (String t : sub2) {
                                    if (!result.contains(t)) {
                                        result.add(t);
                                    }
                                }
                            } else {
                                int[] end2 = new int[]{q};
                                String ident2 = readIdentifier(s, q, end2);
                                if (ident2 != null && !cteNames.contains(ident2) && !result.contains(ident2)) {
                                    result.add(ident2);
                                }
                                q = end2[0];
                            }
                            continue;
                        }
                        // 碰到新子句就退出逗号扩展
                        if (startsWithWord(lower, q, "where") ||
                                startsWithWord(lower, q, "group") ||
                                startsWithWord(lower, q, "order") ||
                                startsWithWord(lower, q, "having") ||
                                startsWithWord(lower, q, "union") ||
                                startsWithWord(lower, q, "qualify") ||
                                startsWithWord(lower, q, "connect") ||
                                startsWithWord(lower, q, "intersect") ||
                                startsWithWord(lower, q, "minus") ||
                                startsWithWord(lower, q, "window") ||
                                startsWithWord(lower, q, "insert")) {
                            break;
                        }
                        q = prev;
                        break;
                    }
                    i = q;
                    continue;
                }
            }

            i = p + 1;
        }

        return result;
    }

    /* -------------------- CTE 收集（WITH 子句） -------------------- */

    private static Set<String> collectCteNames(String s) {
        Set<String> names = new HashSet<>();
        String lower = s.toLowerCase(Locale.ROOT);
        int posWith = indexOfWord(lower, "with", 0);
        if (posWith < 0) {
            return names;
        }
        int i = posWith + 4;
        int n = s.length();

        while (i < n) {
            while (i < n && Character.isWhitespace(s.charAt(i))) { i++; }
            // 读 CTE 名
            int[] end = new int[]{i};
            String cte = readIdentifier(s, i, end);
            if (cte == null || cte.isEmpty()) {
                break;
            }
            names.add(cte);
            i = end[0];

            // 可选列清单 ( ... )
            while (i < n && Character.isWhitespace(s.charAt(i))) { i++; }
            if (i < n && s.charAt(i) == '(') {
                int depth = 1;
                i++;
                while (i < n && depth > 0) {
                    char c = s.charAt(i++);
                    if (c == '(') { depth++; }
                    else if (c == ')') { depth--; }
                }
            }

            // 需要 AS ( ... )
            while (i < n && Character.isWhitespace(s.charAt(i))) { i++; }
            if (startsWithWord(lower, i, "as")) {
                i += 2;
                while (i < n && Character.isWhitespace(s.charAt(i))) { i++; }
                if (i < n && s.charAt(i) == '(') {
                    int depth = 1;
                    i++;
                    while (i < n && depth > 0) {
                        char c = s.charAt(i++);
                        if (c == '(') { depth++; }
                        else if (c == ')') { depth--; }
                    }
                }
            } else {
                break;
            }

            // 如果还有逗号，继续下一个 CTE；否则结束
            while (i < n && Character.isWhitespace(s.charAt(i))) { i++; }
            if (i < n && s.charAt(i) == ',') {
                i++;
                continue;
            } else {
                break;
            }
        }
        return names;
    }

    /* -------------------- 词法/读取工具 -------------------- */

    /** 关键字匹配：两侧不能是“单词字符”（字母/数字/下划线） */
    private static int indexOfWord(String lower, String word, int fromIdx) {
        int i = lower.indexOf(word, fromIdx);
        while (i >= 0) {
            boolean leftOk = (i == 0) || !isWordChar(lower.charAt(i - 1));
            int r = i + word.length();
            boolean rightOk = (r >= lower.length()) || !isWordChar(lower.charAt(r));
            if (leftOk && rightOk) {
                return i;
            }
            i = lower.indexOf(word, i + 1);
        }
        return -1;
    }

    private static boolean startsWithWord(String lower, int idx, String word) {
        if (idx < 0 || idx + word.length() > lower.length()) {
            return false;
        }
        if (!lower.startsWith(word, idx)) {
            return false;
        }
        boolean leftOk = (idx == 0) || !isWordChar(lower.charAt(idx - 1));
        int r = idx + word.length();
        boolean rightOk = (r >= lower.length()) || !isWordChar(lower.charAt(r));
        return leftOk && rightOk;
    }

    private static boolean isWordChar(char ch) {
        return Character.isLetterOrDigit(ch) || ch == '_';
    }

    private static int minPos(int a, int b) { if (a < 0) { return b; } if (b < 0) { return a; } return Math.min(a, b); }

    private static String readIdentifier(String s, int p) { int[] dummy = new int[]{p}; return readIdentifier(s, p, dummy); }

    /** 读取 <schema.>table（支持引号/反引号）；返回表名并通过 endPos[0] 返回结束位置 */
    private static String readIdentifier(String s, int p, int[] endPos) {
        int i = p;
        boolean inQuote = false;
        char qc = '\0';
        StringBuilder sb = new StringBuilder();
        while (i < s.length()) {
            char c = s.charAt(i);
            if (!inQuote && (Character.isWhitespace(c) || c == '(' || c == ')' || c == ',')) {
                break;
            }
            if (c == '`' || c == '"') {
                if (!inQuote) { inQuote = true; qc = c; i++; continue; }
                else if (qc == c) { inQuote = false; qc = '\0'; i++; continue; }
            }
            if (c == ';') { break; }
            sb.append(c);
            i++;
        }
        endPos[0] = i;
        String ident = sb.toString().trim();
        if (ident.isEmpty()) {
            return null;
        }
        ident = ident.replace("`.", ".").replace(".`", ".")
                .replace("\".", ".").replace(".\"", ".")
                .replace("`", "").replace("\"", "");
        int sp = ident.indexOf(' ');
        if (sp > 0) {
            ident = ident.substring(0, sp);
        }
        return ident.toLowerCase(Locale.ROOT);
    }
}
