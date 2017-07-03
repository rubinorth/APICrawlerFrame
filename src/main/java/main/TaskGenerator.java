package main;

import entity.TwiAuthEntity;
import task.DataQueueTask;
import task.GeneralTask;
import task.twiTask.TwiDataQueueTask;
import task.twiTask.TwiTask;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

/**
 * Created by nizeyang on 2016/7/17.
 * 主要负责任务的生成
 */
public class TaskGenerator {

    private final Logger logger = Logger.getLogger("TaskGenerator");

    private BlockingQueue<Object> readDataQueue; //多线程共享数据（来自数据库）队列，一线程写，多线程读
    private BlockingQueue<Object> writeDataQueue; //多线程共享数据队列，多线程写，一线程读

    private Queue<GeneralTask> generalTasks = new LinkedList<>(); //爬虫任务的一个队列
    private Map<Integer, Long> taskTime = new HashMap<>(); //记录每个爬虫的开始时间，Map<任务id, 上一次开始时间>

    private int totalNum; //总共的任务数
    private int currentTaskId = 0; //记录当前提交的任务的编号

    private List<TwiAuthEntity> twiAuthEntities = new ArrayList<>(); //存储twitter相关auth参数

    public TaskGenerator(BlockingQueue<Object> readDataQueue, BlockingQueue<Object> writeDataQueue) {
        this.readDataQueue = readDataQueue;
        this.writeDataQueue = writeDataQueue;
        this.readConfig();
        this.initTaskTime();
    }

    /**
     * 创建一个读写数据的线程
     *
     * @return 读写数据的线程
     */
    public DataQueueTask createReWriTask() {
        switch (MyParam.taskName) {
            case "Twi": {
                return new TwiDataQueueTask(this.readDataQueue, this.writeDataQueue, MyParam.loopTime, MyParam.whichApi);
            }
            default: {
                return null;
            }
        }
    }

    /**
     * 取任务队列最前面的一个任务，并把这个任务从队列中删除
     *
     * @return 下一个任务
     */
    public GeneralTask next() {
        return generalTasks.poll();
    }

    /**
     * 判断是否有新的任务（会创建新的任务）
     * 根据不同的爬取需求，推荐方法如下：
     * 1.如果readDataQueue只是最初写入数据，后面都不会再写入的话，就直接判断readDataQueue是否为空就行。
     * <p>
     * 2.如果爬取的过程中需要对readDataQueue进行更新，则不能只判断readDataQueue是否为空，需要等待。
     * 因为爬api的次数限制是以小时为单位的，所以在这里最多会等一个小时。如果一个小时了都没有新的任务，则认为所有任务都完成了。
     * <p>
     * 另外需要注意以下几个方面：
     * 1.针对方法2，是否可以同时判断readDataQueue和writeDataQueue是不是都为空，都为空的话就说明没有新任务了。
     * 注意，有特殊情况：爬虫线程将readDataQueue的数据都读完了，刚开始爬，还没往writeDataQueue里写。
     * 或者都写在writeDataQueue里了，但也都被读写数据的线程读出去了，没来及往readDataQueue里写。
     * 这两种情况都有可能出现，设置延时再判断的方法也不行，应为读写数据的线程因为要写数据库所以可能很慢。
     * <p>
     * 2.还需要注意hasNext方法有创建新任务的功能，考虑到爬api的次数限制是以小时为单位的，所以会等一小时。
     *
     * @return 是否有新的任务
     */
    public boolean hasNext() {
//        /*
//        直接判断readDataQueue是否为空就行
//         */
//        if(this.readDataQueue.isEmpty()){
//            return false;
//        }

        /*
        等待新任务的生成（爬虫的爬取时间间隔为1小时/15min）
         */
        try {
            long cnt = 0l;
            do {
                if (!this.readDataQueue.isEmpty()) {
                    this.newTask();
                }
                Thread.sleep(2000);
                cnt += 2000;
                if (cnt > MyParam.loopTime) break;
            } while (generalTasks.isEmpty());

        } catch (InterruptedException e) {
            this.logger.info("Exception in TaskGenerator -> hasNext:unknown2");
            e.printStackTrace();
        }

        return !generalTasks.isEmpty();
    }

    /**
     * 创建新的爬取任务
     * 因为爬api的次数限制是以小时为单位的，所以会比较现在的时间和此爬虫上一次运行的时间
     * 若时间之差大于等于1小时，则创建新的任务
     */
    private void newTask() {
        switch (MyParam.taskName) {
            case "Twi": {
                if (System.currentTimeMillis() - this.taskTime.get(this.currentTaskId) >= MyParam.loopTime) {
                    generalTasks.add(new TwiTask(this.currentTaskId, this.twiAuthEntities.get(this.currentTaskId).getConsumerKey(),
                            this.twiAuthEntities.get(this.currentTaskId).getConsumerSecret(),
                            this.twiAuthEntities.get(this.currentTaskId).getAccessToken(),
                            this.twiAuthEntities.get(this.currentTaskId).getAccessTokenSecret(),
                            this.readDataQueue, this.writeDataQueue, MyParam.whichApi));

                    this.taskTime.replace(this.currentTaskId, System.currentTimeMillis());
                    this.currentTaskId = (this.currentTaskId + 1) % this.totalNum;
                }
                break;
            }

            default:
                break;
        }
    }

    /**
     * 读取配置文件
     */
    private void readConfig() {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream("src/main/resources/" + MyParam.taskName + ".properties"));
        } catch (IOException e) {
            this.logger.info("Exception in TaskGenerator -> readConfig:file");
            e.printStackTrace();
        }
        if(MyParam.totalTaskNum == -1) {
            this.totalNum = Integer.parseInt(properties.getProperty("totalNum"));
        }else{
            this.totalNum = MyParam.totalTaskNum;
        }

        switch (MyParam.taskName) {
            case "Twi": {
                for (int i = MyParam.offset; i < this.totalNum + MyParam.offset; i++) {
                    this.twiAuthEntities.add(new TwiAuthEntity(properties.getProperty("consumerKey_" + i),
                            properties.getProperty("consumerSecret_" + i), properties.getProperty("accessToken_" + i),
                            properties.getProperty("accessTokenSecret_" + i)));
                }
                break;
            }
            default:
                break;
        }

        this.logger.info("readConfig finished");
    }

    /**
     * 初始化任务开始时间
     * 注意此时任务都还没开始，所以可以将时间设置为0，而不必设置为当前时间
     */
    private void initTaskTime() {
        for (int i = 0; i < this.totalNum; i++) {
            this.taskTime.put(i, 0l);
        }
    }
}
