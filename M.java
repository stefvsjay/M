package qa;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by lixueyuan1 on 2015/5/6.
 */
public class M {

    private Connection conn = null;
    private Statement stmt = null;

    private String _table = "";
    private String _sql = "";
    private String _where = "";
    private String _limit = "limit 100";
    private String _field = "";

    private String dbErr = "";

    private Map<String, Integer> colTypeMap = new HashMap<String, Integer>();
    private List<String> _cols = new ArrayList<String>();
    private List<String> fieldList = new ArrayList<String>();

    private static final List<String> ResultIsList=new ArrayList<String>();
    private static final List<String> ResultIsNum=new ArrayList<String>();

    static{
        ResultIsList.add("SELECT");
        ResultIsList.add("SHOW");
        ResultIsNum.add("UPDATE");
        ResultIsNum.add("INSERT");
        ResultIsNum.add("DELETE");
        ResultIsNum.add("CREATE");
        ResultIsNum.add("DROP");
    }

    private void field2List() {
        String[] fields = this._field.split(",");
        for (String f : fields) {
            this.fieldList.add(f.trim());
        }
    }

    public void finalize() throws Exception {
        this.conn.close();
    }

    public M(String jdbc, String user, String passwd, String table) throws Exception {
        if (jdbc.contains("?")) {
            jdbc += "&zeroDateTimeBehavior=convertToNull";
        } else {
            jdbc += "?zeroDateTimeBehavior=convertToNull";
        }
        connect(jdbc, user, passwd);
        this._table = table;
        stmt = this.conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        loadInfo();
    }

    private void connect(String jdbc, String user, String passwd) throws Exception {
        conn = DriverManager.getConnection(jdbc, user, passwd);
    }

    private void loadInfo() throws Exception {
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet res = meta.getColumns(null, null, this._table, null);
        if (!res.next()) {
            throw new Exception("cannot get column. table: " + this._table);
        }
        res.beforeFirst();
        int type;
        String colName;
        while (res.next()) {
            type = res.getInt("DATA_TYPE");
            colName = res.getString("COLUMN_NAME");
            this.colTypeMap.put(colName, type);
            this._cols.add(colName);
        }
    }

    private List<String> loadInfo(String table) throws Exception {
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet res = meta.getColumns(null, null, table, null);
        if (!res.next()) {
            throw new Exception("cannot get column. table: " + table);
        }
        res.first();
        List<String> cols=new ArrayList<String>();
        String colName;
        while (res.next()) {
            colName = res.getString("COLUMN_NAME");
            cols.add(colName);
        }
        return cols;
    }

    private String joinStr(String... args) {
        StringBuilder sb = new StringBuilder();
        for (String s : args) {
            sb.append(s).append(" ");
        }
        return sb.toString();
    }

    private void assembleDelete() {
        StringBuilder sb = new StringBuilder();
        sb.append("delete from ").append(this._table);
        if (this._where.length() != 0) {
            sb.append(" where ").append(this._where);
        }
        this._sql = sb.toString();
    }

    private void assembleCount() {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT COUNT(*) FROM ").append(this._table);
        if (this._where.length() != 0) {
            sb.append(" where ").append(this._where);
        }
        this._sql = sb.toString();
    }

    private void assembleSelect() {
        StringBuilder sb = new StringBuilder("SELECT ");
        if (this._field.length() == 0) {
            sb.append("*");
        } else {
            sb.append(this._field);
            field2List();
        }
        sb.append(" FROM ").append(this._table);
        if (this._where.length() != 0) {
            sb.append(" WHERE ").append(this._where);
        }
        if (this._limit.length() != 0) {
            sb.append(" ").append(this._limit);
        }
        this._sql = sb.toString();
    }

    private boolean assembleSum() {
        StringBuilder sb = new StringBuilder("SELECT ");
        if (this._field.length() == 0) {
            return false;
        }
        field2List();
        for(String col:this.fieldList){
            sb.append("sum(").append(this._field).append(") AS ").append(col).append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(" FROM ").append(this._table);
        if (this._where.length() != 0) {
            sb.append(" WHERE ").append(this._where);
        }
        this._sql = sb.toString();
        return true;
    }

    private void assembleSave(Map<String, Object> data) {
        StringBuilder sb = new StringBuilder("UPDATE ");
        sb.append(this._table);
        sb.append(" SET ");
        for (String key : data.keySet()) {
            sb.append(key).append("=\"");
            sb.append(data.get(key)).append("\",");
        }
        sb.deleteCharAt(sb.length() - 1);
        if (this._where.length() != 0) {
            sb.append(" WHERE ").append(this._where);
        }
        this._sql = sb.toString();
    }

    private void assembleAdd(Map<String, Object> data) {
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        sb.append(this._table);
        StringBuilder keys = new StringBuilder(" (");
        StringBuilder values = new StringBuilder(" (");
        for (String key : data.keySet()) {
            keys.append(key).append(",");
            values.append("\"").append(data.get(key)).append("\"").append(",");
        }
        keys.deleteCharAt(keys.length() - 1).append(")");
        values.deleteCharAt(values.length() - 1).append(");");
        sb.append(keys).append(" VALUES").append(values);
        this._sql = sb.toString();
    }

    private void assembleAdd(Object[] data) {
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        sb.append(this._table);
        sb.append(" VALUES (");
        for (Object o : data) {
            if (o == null) {
                sb.append("null").append(",");
            } else {
                sb.append("\"").append(o.toString()).append("\",");
            }
        }
        sb.deleteCharAt(sb.length() - 1).append(")");
        this._sql = sb.toString();
    }

    private int doExec() {
        int res = -1;
        try {
            System.out.println("execute sql: " + this._sql);
            res = this.stmt.executeUpdate(this._sql);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("catch exception while executing sql: " + this._sql);
        } finally {
            System.out.println("affected rows: " + res);
            clear();
        }
        return res;
    }

    private int doCount() {
        int res = -1;
        try {
            System.out.println("execute sql: " + this._sql);
            ResultSet rs = this.stmt.executeQuery(this._sql);
            rs.next();
            res = rs.getInt("count(*)");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("catch exception while executing sql: " + this._sql);
        } finally {
            clear();
        }
        return res;
    }


    private List<Map<String, Object>> doSelect() {
        List<Map<String, Object>> res = new ArrayList<Map<String, Object>>();
        ResultSet rs;
        try {
            System.out.println("execute sql: " + this._sql);
            rs = this.stmt.executeQuery(this._sql);
            res = rs2MapList(rs, this._cols);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("catch exception while executing sql: " + this._sql);
        } finally {
            System.out.println("selected rows: " + res.size());
            clear();
        }
        return res;
    }

    private List<Map<String, Object>> doSelect(String sql,List<String> cols) {
        List<Map<String, Object>> res = new ArrayList<Map<String, Object>>();
        ResultSet rs;
        try {
            System.out.println("execute sql: " + sql);
            rs = this.stmt.executeQuery(sql);
            res = rs2MapList(rs,cols);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("catch exception while executing sql: " + sql);
        } finally {
            System.out.println("selected rows: " + res.size());
            clear();
        }
        return res;
    }

    private Map<String, Object> doFind() {
        Map<String, Object> res = new HashMap<String, Object>();
        ResultSet rs;
        try {
            System.out.println("execute sql: " + this._sql);
            rs = this.stmt.executeQuery(this._sql);
            res = rs2Map(rs, this._cols);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("catch exception while executing sql: " + this._sql);
        } finally {
            System.out.println("selected rows: " + (res==null?0:1));
            clear();
        }
        return res;
    }

    private Object doSum(){
        Object o=null;
        try {
            System.out.println("execute sql: " + this._sql);
            ResultSet rs = this.stmt.executeQuery(this._sql);
            if(rs.next()) {
                o = rs.getObject(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("catch exception while executing sql: " + this._sql);
        } finally {
            System.out.println("selected rows: 1");
            clear();
        }
        return o;
    }

    private List<Map<String, Object>> rs2MapList(ResultSet rs,List<String> cols) throws Exception {
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        List<String> cs = fieldList.isEmpty() ? cols : fieldList;
        while (rs.next()) {
            Map<String, Object> m = new HashMap<String, Object>();
            for (String col : cs) {
                m.put(col, rs.getObject(col));
            }
            list.add(m);
        }
        return list;
    }

    private Map<String, Object> rs2Map(ResultSet rs,List<String> cols) throws Exception {
        Map<String, Object> m=null;
        List<String> cs = fieldList.isEmpty() ? cols : fieldList;
        if (rs.next()) {
            m = new HashMap<String, Object>();
            for (String col : cs) {
                m.put(col, rs.getObject(col));
            }
        }
        return m;
    }

    private void clear() {
        this._where = "";
        this._field="";
        this._limit= "limit 100";
        this.fieldList.clear();
    }

    /**
     * @param logicP       true:AND  false:OR
     * @param newCondition  condition
     * @return   return this
     */
    public M where(boolean logicP, String newCondition) {
        String logic = logicP ? "AND" : "OR";
        if (this._where.length() == 0) {
            this._where = newCondition;
        } else {
            this._where = joinStr(this._where, logic, newCondition);
        }
        return this;
    }

    public M where(String newCondition) {
        return where(true, newCondition);
    }

    public M where(String k, String v) {
        return where(true, k+"=\""+v+"\"");
    }

    public M where(String k, Object v) {
        return where(true, k+"=\""+v.toString()+"\"");
    }

    public M where(boolean logicP, String k, String v) {
        String logic = logicP ? "AND" : "OR";
        StringBuilder sb = new StringBuilder();
        sb.append(logic).append(" ").append(k).append("=\"").append(v).append("\"");
        return where(true, sb.toString());
    }

    public M where(boolean logicP, String k, Object v) {
        String logic = logicP ? "AND" : "OR";
        StringBuilder sb = new StringBuilder();
        sb.append(logic).append(" ").append(k).append("=\"").append(v).append("\"");
        return where(true, sb.toString());
    }

    public M limit(int len) {
        if (len > 0) {
            this._limit = "limit " + len;
        }
        return this;
    }

    public M limit(int begin, int len) {
        if ((begin >= 0) && (len > 0)) {
            this._limit = "limit "+begin+","+len;
        }
        return this;
    }

    public M field(String f) {
        if (this._field.length() == 0) {
            this._field = f;
        } else {
            this._field += "," + f;
        }
        return this;
    }

    public int del() {
        assembleDelete();
        return doExec();
    }

    public int count() {
        assembleCount();
        return doCount();
    }

    public int save(String key,Object value){
        Map<String,Object> data=new HashMap<String, Object>();
        data.put(key,value);
        return save(data);
    }

    public int save(Map<String, Object> data) {
        assembleSave(data);
        return doExec();
    }

    public int add(Map<String, Object> data) {
        assembleAdd(data);
        return doExec();
    }

    public int add(Object[] data) {
        assembleAdd(data);
        return doExec();
    }

    public List<Map<String, Object>> select() {
        assembleSelect();
        return doSelect();
    }

    public Map<String, Object> find(){
        limit(1);
        assembleSelect();
        return doFind();
    }

    public List<Map<String, Object>> multiSum() {
        assembleSum();
        return doSelect();
    }

    public List<Map<String, Object>> multiSum(String fields) {
        this._field=fields;
        assembleSum();
        return doSelect();
    }

    public Object sum(String field){
        this._field=field;
        assembleSum();
        return doSum();
    }

    //  --------------------   static below    --------------------

    public List<Map<String, Object>> executQuery(final String sql){
        if(!ResultIsList.contains(sql.toUpperCase().split("\\s+",2)[0])){
            return null;
        }
        int pos1=sql.indexOf("from");
        int pos2=sql.indexOf("where");
        String table=sql.substring(pos1+4,pos2).trim();
        List<String> cols= null;
        try {
            cols = loadInfo(table);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return doSelect(sql, cols);
    }

    public int executeUpdate(final String sql){
        if(!ResultIsNum.contains(sql.toUpperCase().split("\\s+", 2)[0])){
            return -1;
        }
        this._sql=sql;
        return doExec();
    }

    public static boolean load(M m, String filename) {
        File file = new File(filename);
        String line;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            while ((line = reader.readLine()) != null) {
                Object[] os = dealWithLine(line);
                m.add(os);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static boolean source(M m, String filename){
        File file = new File(filename);
        String line;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            while ((line = reader.readLine()) != null) {
                m.stmt.addBatch(line);
            }
            int[] rows=m.stmt.executeBatch();
            System.out.println("debug");
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private static Object[] dealWithLine(String line) {
        String[] os = line.split("\t");
        for (int i = 0; i < os.length; i++) {
            if (os[i].equals("")) {
                os[i] = null;
            }
        }
        return os;
    }

    public static String map2String(Map<?, ?> map) {
        StringBuilder sb = new StringBuilder();
        for (Object key : map.keySet()) {
            sb.append(key).append(" : ").append(map.get(key)).append("\n");
        }
        return sb.toString();
    }

    public boolean changeTable(String newTableName){
        this._table=newTableName;
        this._cols.clear();
        this.colTypeMap.clear();
        try {
            loadInfo();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
