package util;

import java.sql.*;

/**
 * Created by nizeyang on 2016/3/10.
 * 数据库操作工具类
 */
public class SqlUtil {

    private Connection conn;
    private boolean isBatch = false; // 标记PreparedStatement是否调用了addBatch

    public SqlUtil(String jdbc, String url) {
        try {
            Class.forName(jdbc);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        this.connectToSql(url);
    }

    /**
     * 选择操作
     *
     * @param sql sql语句
     * @return ResultSet
     */
    public ResultSet selectFromSql(String sql) {
        ResultSet res = null;
        PreparedStatement ps;
        try {
            ps = this.conn.prepareStatement(sql);
            res = ps.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return res;
    }

    /**
     * 插入操作
     *
     * @param sql sql语句
     * @return 是否成功
     */
    public boolean insertIntoSql(String sql) {
        PreparedStatement ps;
        try {
            ps = this.conn.prepareStatement(sql);
            this.insertIntoSql(ps);
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 插入操作，当插入语句较复杂或者需要一次性插入多条数据时使用本方法
     *
     * @param ps PreparedStatement对象
     * @return 是否成功
     */
    public boolean insertIntoSql(PreparedStatement ps) {
        try {
            if (this.isBatch) {
                this.conn.setAutoCommit(false);
                ps.executeBatch();
                this.conn.commit();
                this.conn.setAutoCommit(true);
            } else {
                ps.executeUpdate();
            }
            ps.close();
            return true;
        } catch (BatchUpdateException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            this.conn.setAutoCommit(true);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 更新操作
     *
     * @param sql sql语句
     * @return 是否成功
     */
    public boolean updateSql(String sql) {
        return this.insertIntoSql(sql);
    }

    /**
     * 更新操作
     *
     * @param ps PreparedStatement对象
     * @return 是否成功
     */
    public boolean updateSql(PreparedStatement ps) {
        return this.insertIntoSql(ps);
    }

    /**
     * 获得一个PreparedStatement对象
     *
     * @param sql sql语句
     * @return PreparedStatement对象
     */
    public PreparedStatement getPs(String sql) {
        PreparedStatement ps = null;
        try {
            ps = this.conn.prepareStatement(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ps;
    }

    /**
     * 设置PreparedStatement对象
     *
     * @param ps     PreparedStatement对象
     * @param method 略
     * @param index  略
     * @param data   略
     */
    public void setPs(PreparedStatement ps, String method, int index, Object data) {
        try {
            switch (method) {
                case "setString": {
                    ps.setString(index, (String) data);
                    break;
                }
                case "setInt": {
                    ps.setInt(index, (Integer) data);
                    break;
                }
                case "setLong": {
                    ps.setLong(index, (Long) data);
                    break;
                }
                case "setDate": {
                    ps.setDate(index, (Date) data);
                    break;
                }
                default:
                    break;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 若需要一次性插入多条数据时调用本方法
     *
     * @param ps PreparedStatement对象
     */
    public void myAddBatch(PreparedStatement ps) {
        this.isBatch = true;
        try {
            ps.addBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void disconnnect() {
        try {
            this.conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void connectToSql(String url) {
        try {
            this.conn = DriverManager.getConnection(url);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
