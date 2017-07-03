package main;

import java.util.concurrent.*;
import java.util.logging.Logger;

/**
 * Created by Dell on 2016/7/17.
 * 爬虫从这里开始运行
 * 整个运行框架为：
 * 1.一个线程从数据库中或文件中读取数据（如用户id之类），放到队列readDataQueue中
 * 2.多线程从readDataQueue中取数据，然后访问api，爬取数据，将其写入队列writeDataQueue中
 * 3.同一个线程从writeDataQueue中取数据，将其写入数据库或文件中，也可以写入readDataQueue中，形成循环
 */
public class TaskPool {

    private final Logger logger = Logger.getLogger("TaskPool");

    private ExecutorService taskpool; //线程池
    private BlockingQueue<Object> readDataQueue; //多线程共享数据（来自数据库）队列，一线程写，多线程读
    private BlockingQueue<Object> writeDataQueue; //多线程共享数据队列，多线程写，一线程读
    private TaskGenerator taskGenerator; //任务生成器

    public TaskPool() {
        this.taskpool = Executors.newFixedThreadPool(MyParam.NCPU * 2); //IO密集型任务，一般将线程数设置为NCPU的两倍
        this.readDataQueue = new LinkedBlockingQueue<>(MyParam.readDataQueueCapacity);
        this.writeDataQueue = new LinkedBlockingQueue<>();
    }

    public void missionStart() {
        this.logger.info("missionStart");

        this.taskGenerator = new TaskGenerator(this.readDataQueue, this.writeDataQueue);

        this.taskpool.execute(this.taskGenerator.createReWriTask()); //创建数据读写线程

        /*
        不断创建爬虫线程（采用了线程池，所以这里最好叫“爬虫任务”）直到没有新的爬取任务
         */
        while (this.taskGenerator.hasNext()) {
            this.taskpool.execute(this.taskGenerator.next());
        }
    }

    public void missionComplete() {
        this.taskpool.shutdown(); //还在跑的线程会跑完后结束
        this.logger.info("missionComplete");
    }
}
