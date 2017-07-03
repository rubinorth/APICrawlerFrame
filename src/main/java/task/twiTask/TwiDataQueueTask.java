package task.twiTask;

import entity.twiEntity.TwiFollowEntity;
import entity.twiEntity.TwiProfileEntity;
import entity.twiEntity.TwiTweetsEntity;
import main.MyParam;
import task.DataQueueTask;
import twitter4j.Status;
import util.BloomFilter;
import util.EmojiFilter;
import util.SqlUtil;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

/**
 * Created by nizeyang on 2016/7/22.
 * DataQueueTask的子类，操作Twi数据
 */
public class TwiDataQueueTask extends DataQueueTask {

    private final Logger logger = Logger.getLogger("TwiDataQueueTask");
    private SqlUtil sqlUtil;
    BloomFilter<String> bloomFilter; //用于去重

    public TwiDataQueueTask(BlockingQueue<Object> readDataQueue, BlockingQueue<Object> writeDataQueue, long l, String whichApi) {
        super(readDataQueue, writeDataQueue, l, whichApi);
        this.init();
    }

    @Override
    protected void init() {
        double falsePositiveProbability = 0.01;
        int expectedSize = 1000000;
        this.bloomFilter = new BloomFilter<>(falsePositiveProbability, expectedSize);

        String url = "jdbc:mysql://localhost:3306/twitter?"
                + "user=root&password=root&useUnicode=true&characterEncoding=UTF8";

        this.sqlUtil = new SqlUtil("com.mysql.jdbc.Driver", url);
        this.twiRead();
    }

    @Override
    protected void dataProcessing() {
        this.logger.info("dataProcessing start");
        switch (this.whichApi) {
            case "followee": {
                this.twiFollowWrite();
                break;
            }
            case "profile": {
                this.twiProfileWrite();
                break;
            }
            case "tweet": {
                this.twiUserTweetsWrite();
                break;
            }
            default:
                break;
        }
        this.logger.info("dataProcessing finished");
    }

    /**
     * 从数据库中读取数据，初始化时调用，视不同爬取需求里面的内容可做相应变化
     * 如果需要循环爬取的话，在写入数据库的方法内部也会调用此方法
     */
    private void twiRead() {
        String sql = "";
        switch (this.whichApi) {
            case "followee": {
                sql = "SELECT uid, followeeCursor FROM user WHERE followeeStatus = 'undone' ORDER BY id asc limit 0,2000";
                break;
            }
            case "profile": {
                sql = "SELECT uid FROM user WHERE id < 110000 AND followeeStatus = 'done' AND uid NOT IN (SELECT uid from profile)";
                break;
            }
            case "tweet": {
                sql = "SELECT uid, maxId FROM user WHERE tweetStatus = 'undone' ORDER BY id asc limit 0,1000";
                break;
            }
            default:
                break;
        }
        this.logger.info("reading from sql finished");
        ResultSet res = this.sqlUtil.selectFromSql(sql);
        String tmp;
        try {
            switch (this.whichApi) {
                case "followee": {
                    synchronized (this.readDataQueue) {
                        while (res.next() && this.readDataQueue.remainingCapacity() >= MyParam.readDataQueueCapacity / 20) {
                            tmp = res.getString("uid") + "&" + res.getString("followeeCursor");
                            if (!this.bloomFilter.contains(tmp)) {
                                this.bloomFilter.add(tmp);
                                this.readDataQueue.add(tmp);
                            }
                        }
                    }
                    break;
                }
                case "profile": {
                    synchronized (this.readDataQueue) {
                        while (res.next() && this.readDataQueue.remainingCapacity() >= MyParam.readDataQueueCapacity / 20) {
                            tmp = res.getString("uid");
                            if (!this.bloomFilter.contains(tmp)) {
                                this.bloomFilter.add(tmp);
                                this.readDataQueue.add(Long.parseLong(tmp));
                            }
                        }
                    }
                    break;
                }
                case "tweet": {
                    synchronized (this.readDataQueue) {
                        while (res.next() && this.readDataQueue.remainingCapacity() >= MyParam.readDataQueueCapacity / 20) {
                            tmp = res.getString("uid") + "&" + res.getString("maxId");
                            if (!this.bloomFilter.contains(tmp)) {
                                this.bloomFilter.add(tmp);
                                this.readDataQueue.add(tmp);
                            }
                        }
                    }
                    break;
                }
                default:
                    break;
            }
            res.close();
        } catch (SQLException e) {
            this.logger.info("Exception in ReadTask -> run:sql error");
            e.printStackTrace();
        }
    }

    /**
     * 将用户id和关注关系数据存入数据库中
     */
    private void twiFollowWrite() {
        List<String> sqls = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            sqls.add("INSERT INTO " + this.whichApi + "_" + Integer.toString(i) + " (uid, " + this.whichApi + ") VALUES(?, ?)");
        }
        List<PreparedStatement> pss = new ArrayList<>();

        TwiFollowEntity twiFollowEntity;

        while (this.hasNext()) {
            System.out.println(this.writeDataQueue.size());
            twiFollowEntity = (TwiFollowEntity) this.writeDataQueue.poll();

            /*
            爬到的新的uid需要存入数据库
             */
            String sql = "INSERT INTO user SET uid = ?";
            PreparedStatement ps = this.sqlUtil.getPs(sql);
            for (Long uid : twiFollowEntity.getNewUsers()) {
                this.sqlUtil.setPs(ps, "setLong", 1, uid);
                this.sqlUtil.myAddBatch(ps);
            }
            this.sqlUtil.insertIntoSql(ps);

            /*
            将关注关系存到数据库中
             */
            pss.clear();
            for (int i = 0; i < 10; i++) {
                pss.add(this.sqlUtil.getPs(sqls.get(i)));
            }
            int whichTable;
            for (Long uid : twiFollowEntity.getEdges().keySet()) {
                for (Long follow : twiFollowEntity.getEdges().get(uid)) {
                    whichTable = (int) (uid % 10l);
                    this.sqlUtil.setPs(pss.get(whichTable), "setLong", 1, uid);
                    this.sqlUtil.setPs(pss.get(whichTable), "setLong", 2, follow);
                    this.sqlUtil.myAddBatch(pss.get(whichTable));
                }
            }
            for (int i = 0; i < 10; i++) {
                this.sqlUtil.insertIntoSql(pss.get(i));
            }

            /*
            种子uid处理
             */
            sql = "UPDATE user SET " + this.whichApi + "Status = ?, " + this.whichApi + "Cursor = ? WHERE uid = ?";
            ps = this.sqlUtil.getPs(sql);
            for (Long uid : twiFollowEntity.getOldUsers()) {
                if (twiFollowEntity.getF().contains(uid)) { //设置了隐私权限的用户id
                    this.sqlUtil.setPs(ps, "setString", 1, "forbidden");
                    this.sqlUtil.setPs(ps, "setLong", 2, -1l);
                } else if (twiFollowEntity.getUc().get(uid) != 0) { // 没爬完的用户
                    this.sqlUtil.setPs(ps, "setString", 1, "undone");
                    this.sqlUtil.setPs(ps, "setLong", 2, twiFollowEntity.getUc().get(uid));
                } else {
                    this.sqlUtil.setPs(ps, "setString", 1, "done"); // 爬完的用户
                    this.sqlUtil.setPs(ps, "setLong", 2, twiFollowEntity.getUc().get(uid));
                }
                this.sqlUtil.setPs(ps, "setLong", 3, uid);
                this.sqlUtil.myAddBatch(ps);
            }
            for (Long uid : twiFollowEntity.getF()) {
                this.sqlUtil.setPs(ps, "setString", 1, "forbidden");
                this.sqlUtil.setPs(ps, "setLong", 2, -1l);
                this.sqlUtil.setPs(ps, "setLong", 3, uid);
                this.sqlUtil.myAddBatch(ps);
            }
            this.sqlUtil.updateSql(ps);

            /*
            读取新的uid到readDataQueue中
             */
            if (this.readDataQueue.remainingCapacity() >= MyParam.readDataQueueCapacity / 2) {
                this.twiRead();
            }

//            System.out.println("end");
        }
        this.sqlUtil.disconnnect();
    }

    /**
     * 将用户数据存入数据库中
     */
    private void twiProfileWrite() {
        TwiProfileEntity twiProfileEntity;

        while (this.hasNext()) {
            System.out.println(this.writeDataQueue.size());
            twiProfileEntity = (TwiProfileEntity) this.writeDataQueue.poll();

            /*
            新的profile
             */
            String sql = "INSERT INTO profile SET jsonText = ? ";
            PreparedStatement ps = this.sqlUtil.getPs(sql);
            for (Long uid : twiProfileEntity.getUp().keySet()) {
                this.sqlUtil.setPs(ps, "setString", 1, twiProfileEntity.getUp().get(uid).toString());

                this.sqlUtil.myAddBatch(ps);
            }
            this.sqlUtil.updateSql(ps);

            /*
            更新user表的status
             */
            sql = "UPDATE user SET profileStatus = ? WHERE uid = ?";
            ps = this.sqlUtil.getPs(sql);
            for (Long uid : twiProfileEntity.getUp().keySet()) {
                this.sqlUtil.setPs(ps, "setString", 1, "done");
                this.sqlUtil.setPs(ps, "setLong", 2, uid);
                this.sqlUtil.myAddBatch(ps);
            }
            this.sqlUtil.updateSql(ps);

//            if (this.readDataQueue.remainingCapacity() >= MyParam.readDataQueueCapacity / 2) {
//                this.twiRead();
//            }
        }
        this.sqlUtil.disconnnect();
    }

    /**
     * 将tweets存入数据库，并更新user表的status
     */
    private void twiUserTweetsWrite() {
        List<String> sqls = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            sqls.add("INSERT INTO tweet_" + Integer.toString(i) + " (jsonText) VALUES(?)");
        }

        while (this.hasNext()) {
            if (this.writeDataQueue.size() > 45) {
                System.out.println(this.writeDataQueue.size());
            }
            TwiTweetsEntity twiTweetsEntity = (TwiTweetsEntity) this.writeDataQueue.poll();

            /*
            爬到的tweets需要存入数据库
             */
            Long uid = twiTweetsEntity.getUid();
            if (!twiTweetsEntity.isEmpty()) {
                int whichTable = (int) (uid % 10l);
                PreparedStatement tmpPs = this.sqlUtil.getPs(sqls.get(whichTable));
                for (Status s : twiTweetsEntity.getStatus()) {

                    this.sqlUtil.setPs(tmpPs, "setString", 1, EmojiFilter.filterEmoji(s.toString(), " "));

                    this.sqlUtil.myAddBatch(tmpPs);
                }
                this.sqlUtil.insertIntoSql(tmpPs);
            }


            /*
            种子uid的status需要更新，标明其tweets已爬完
             */
            String sql = "UPDATE user SET tweetStatus = ?, maxId = ? WHERE uid = ?";
            PreparedStatement ps = this.sqlUtil.getPs(sql);

            this.sqlUtil.setPs(ps, "setString", 1, twiTweetsEntity.getS());
            this.sqlUtil.setPs(ps, "setLong", 2, twiTweetsEntity.getMaxId());
            this.sqlUtil.setPs(ps, "setLong", 3, uid);
            this.sqlUtil.myAddBatch(ps);

            this.sqlUtil.updateSql(ps);

            if (this.readDataQueue.remainingCapacity() >= MyParam.readDataQueueCapacity / 2) {
                this.twiRead();
            }
        }
        this.sqlUtil.disconnnect();
    }


}
