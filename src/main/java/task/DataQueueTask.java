package task;

import main.MyParam;

import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

/**
 * Created by nizeyang on 2016/7/20.
 * 读写数据的线程
 * 负责操作readDataQueue和writeDataQueue
 */
public abstract class DataQueueTask implements Runnable {

    private final Logger logger = Logger.getLogger("DataQueueTask");
    protected String whichApi;
    protected BlockingQueue<Object> readDataQueue;
    protected BlockingQueue<Object> writeDataQueue;
    private long loopTime;


    public DataQueueTask(BlockingQueue<Object> readDataQueue, BlockingQueue<Object> writeDataQueue, long l, String whichApi) {
        this.whichApi = whichApi;
        this.readDataQueue = readDataQueue;
        this.writeDataQueue = writeDataQueue;
        this.loopTime = l;
    }

    @Override
    public void run() {
        this.dataProcessing();
    }

    /**
     * 初始化工作，一般为从数据库中读取数据，将其写入readDataQueue中
     */
    protected abstract void init();

    /**
     * 从writeDataQueue中读取数据，然后写到数据库中或者再写入readDataQueue中
     */
    protected abstract void dataProcessing();

    /**
     * 判断writeDataQueue队列中还有没有数据需要写入数据库
     * <p>
     * 因为爬虫每小时次数的限制，所以readDataQueue队列不空的话就需要不断循环等待爬虫线程写writeDataQueue了。
     * 不能以readDataQueue和writeDataQueue是否同时为空来判断有没有新的数据是不靠谱的，因为很可能由于网速原因，爬虫
     * 卡在与api的交互过程中，此时爬虫线程读了readDataQueue还没能往writeDataQueue写。
     *
     * @return 队列是否还有下一个数据
     */
    protected boolean hasNext() {
        try {
            long cnt = 0l;
            while (this.writeDataQueue.isEmpty()) {
                Thread.sleep(500);
                cnt += 500;
                if (cnt > this.loopTime) break;
            }
        } catch (InterruptedException e) {
            this.logger.info("Exception in WriteTask -> hasNext:unknown");
            e.printStackTrace();
        }
        return !this.writeDataQueue.isEmpty();
    }
}


