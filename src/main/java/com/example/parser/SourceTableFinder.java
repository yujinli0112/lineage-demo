package com.example.parser;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;

import java.lang.reflect.Method;
import java.util.*;

/**
 * 收集 SELECT 的来源表（排除 CTE 名），
 * 兼容 JSqlParser 4.x / 5.x 的不同 API：
 * - 有的版本 TablesNamesFinder.findTables(...) / getTableList(...) 接受 Select/Statement
 * - 有的版本只接受 String
 * 因此统一用反射调用，并在必要时传入 select.toString()。
 */
public class SourceTableFinder {

    /** 对外主入口：给一个 Select，返回来源表集合（已统一小写、去引号、排除 CTE 名） */
    /** 返回规范化后的表名集合（会走完整 AST，覆盖子查询/JOIN/UNION 等） */
    public Set<String> getSourceTables(Select select) {
        if (select == null) {
            return Set.of();
        }
        TablesNamesFinder finder = new TablesNamesFinder(); // JSqlParser 5.x
        List<String> names = finder.getTableList((Statement) select);

        Set<String> out = new LinkedHashSet<>();
        for (String n : names) {
            if (n == null) {
                continue;
            }
            String norm = normalize(n);
            if (isIgnorable(norm)) {
                continue;
            }
            out.add(norm);
        }
        return out;
    }

    private boolean isIgnorable(String n) {
        if (n == null) {
            return true;
        }
        String s = n.trim().toLowerCase(Locale.ROOT);
        if (s.isEmpty()) {
            return true;
        }
        // 需要的话可以在这里排除系统表
        return "dual".equals(s);
    }



    /** 也支持传 Statement 或 SQL 字符串（可选） */
    public Set<String> getSourceTablesFrom(Object stmtOrSql) {
        return getSourceTablesInternal(stmtOrSql);
    }

    /* -------------------- 内部实现 -------------------- */

    private Set<String> getSourceTablesInternal(Object selectOrStmtOrSql) {
        // 1) 先收集 CTE 名（WITH 子句里的临时表名），后面从来源列表里排除
        Set<String> cteNames = collectCteNames(selectOrStmtOrSql);

        // 2) 用 TablesNamesFinder 的各种可能方法拉表名（反射兼容不同签名）
        List<String> raw = invokeTablesNamesFinder(selectOrStmtOrSql);

        // 3) 规范化 + 排除 CTE 名
        Set<String> result = new LinkedHashSet<>();
        for (String t : raw) {
            String n = normalize(t);
            if (!cteNames.contains(n)) {
                result.add(n);
            }
        }
        return result;
    }

    /** 反射调用 TablesNamesFinder (静态/实例) 的 findTables/getTableList，兼容不同参数签名 */
    @SuppressWarnings("unchecked")
    private List<String> invokeTablesNamesFinder(Object arg) {
        try {
            Class<?> util = Class.forName("net.sf.jsqlparser.util.TablesNamesFinder");

            // 优先尝试：静态 findTables(参数类型跟 arg 一致)
            try {
                Method m = util.getMethod("findTables", arg.getClass());
                Object v = m.invoke(null, arg);
                return toStringList(v);
            } catch (NoSuchMethodException ignored) {}

            // 其次：静态 findTables(String)
            try {
                Method m = util.getMethod("findTables", String.class);
                Object v = m.invoke(null, String.valueOf(arg));
                return toStringList(v);
            } catch (NoSuchMethodException ignored) {}

            // 再次：实例 getTableList(参数类型跟 arg 一致)
            Object inst = util.getDeclaredConstructor().newInstance();
            try {
                Method m = util.getMethod("getTableList", arg.getClass());
                Object v = m.invoke(inst, arg);
                return toStringList(v);
            } catch (NoSuchMethodException ignored) {}

            // 最后：实例 getTableList(String)
            try {
                Method m = util.getMethod("getTableList", String.class);
                Object v = m.invoke(inst, String.valueOf(arg));
                return toStringList(v);
            } catch (NoSuchMethodException ignored) {}

        } catch (Exception e) {
            // 忽略，走空列表
        }
        return Collections.emptyList();
    }

    private List<String> toStringList(Object v) {
        if (v instanceof List<?> list) {
            List<String> out = new ArrayList<>();
            for (Object o : list) {
                if (o != null) {
                    out.add(String.valueOf(o));
                }
            }
            return out;
        }
        return Collections.emptyList();
    }

    /** 通过反射读取 WITH 子句，收集 CTE 名（兼容 4.x/5.x 两种字段/方法形态） */
    private Set<String> collectCteNames(Object selectOrStmtOrSql) {
        Set<String> names = new HashSet<>();
        Object selectObj = selectOrStmtOrSql;

        // 如果传进来的是字符串，没法拿 AST，只能放弃 CTE 排除
        if (selectObj instanceof String) {
            return names;
        }

        // 直接尝试在 Select 上找 getWithItemsList()
        try {
            Method m = selectObj.getClass().getMethod("getWithItemsList");
            Object list = m.invoke(selectObj);
            extractWithList(names, list);
            return names;
        } catch (Exception ignored) {}

        // 尝试：先拿 getSelectBody() 再取 getWithItemsList()
        try {
            Method mBody = selectObj.getClass().getMethod("getSelectBody");
            Object body = mBody.invoke(selectObj);
            if (body != null) {
                try {
                    Method mList = body.getClass().getMethod("getWithItemsList");
                    Object list = mList.invoke(body);
                    extractWithList(names, list);
                } catch (Exception ignored2) {}
            }
        } catch (Exception ignored) {}

        return names;
    }

    @SuppressWarnings("unchecked")
    private void extractWithList(Set<String> names, Object list) {
        if (!(list instanceof List<?> l)) {
            return;
        }
        for (Object wi : l) {
            String name = resolveWithItemName(wi);
            if (name != null) {
                names.add(normalize(name));
            }
        }
    }

    /** 兼容 4.x/5.x 的 WithItem 名称获取：getName() / getAliasName() / getAlias().getName() */
    private String resolveWithItemName(Object wi) {
        try { // 4.x
            Method m = wi.getClass().getMethod("getName");
            Object v = m.invoke(wi);
            if (v instanceof String s && !s.isEmpty()) {
                return s;
            }
        } catch (Exception ignored) {}
        try { // 5.x
            Method m = wi.getClass().getMethod("getAliasName");
            Object v = m.invoke(wi);
            if (v instanceof String s && !s.isEmpty()) {
                return s;
            }
        } catch (Exception ignored) {}
        try { // 5.x 另一条路径
            Method m1 = wi.getClass().getMethod("getAlias");
            Object alias = m1.invoke(wi);
            if (alias != null) {
                Method m2 = alias.getClass().getMethod("getName");
                Object v = m2.invoke(alias);
                if (v instanceof String s && !s.isEmpty()) {
                    return s;
                }
            }
        } catch (Exception ignored) {}
        return null;
    }

    /** 标准化：去引号 → 合并库表分隔 → 小写 */
    private String normalize(String name) {
        String n = name.trim();
        if (n.startsWith("`") && n.endsWith("`")) {
            n = n.substring(1, n.length() - 1);
        }
        if (n.startsWith("\"") && n.endsWith("\"")) {
            n = n.substring(1, n.length() - 1);
        }
        n = n.replace("`.", ".").replace(".`", ".").replace("\".", ".").replace(".\"", ".");
        return n.toLowerCase(Locale.ROOT);
    }
}
