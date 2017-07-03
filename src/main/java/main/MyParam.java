package main;

/**
 * Created by nizeyang on 2016/9/13.
 * 爬虫从这里开始运行
 */
public class MyParam {
    public static int NCPU = 3; //cpu的核数（线程数？）
    public static final int readDataQueueCapacity = 1000; // readDataQueue的容量
    public static int totalTaskNum = 30; //总共的任务数
    public static int offset = 0; //记录任务偏移量

    public static final String taskName = "Twi"; //爬虫任务的名称
    public static final String whichApi = "tweet"; //具体访问的是哪个api

    public static long loopTime = 900000l; // 每个爬虫任务两次爬取的时间间隔，毫秒

    public static boolean isFinished = false;

    public static void main(String[] args) {
        TaskPool tp = new TaskPool();
        tp.missionStart();
        tp.missionComplete();
    }
}
