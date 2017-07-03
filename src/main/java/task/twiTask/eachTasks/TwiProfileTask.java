package task.twiTask.eachTasks;

import entity.twiEntity.TwiProfileEntity;
import task.SpecificTask;
import twitter4j.*;
import twitter4j.internal.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Created by nizeyang on 2016/9/10.
 * 爬取用户信息
 */
public class TwiProfileTask implements SpecificTask {

    private final Logger logger = Logger.getLogger(this.getClass());
    private Twitter twitter;
    private int rate;
    private BlockingQueue<Object> readDataQueue;
    private BlockingQueue<Object> writeDataQueue;
    private List<Long> uids;

    public TwiProfileTask(Twitter t, int c, BlockingQueue<Object> readDataQueue, BlockingQueue<Object> writeDataQueue) {
        this.twitter = t;
        this.rate = c;
        this.readDataQueue = readDataQueue;
        this.writeDataQueue = writeDataQueue;
        this.init();
    }

    @Override
    public void crawl() {
        /*
        遍历uids列表，访问api，将获得的profile放到twiProfileEntity中，再写入writeDataQueue中
         */
        TwiProfileEntity twiProfileEntity = new TwiProfileEntity();
        for (Long uid : this.uids) {
            try {
                twiProfileEntity.add(uid, this.twitter.showUser(uid));
            } catch (TwitterException e) {
                if(e.getStatusCode() == 429){ // 次数超限
                    this.logger.error("Exception in TwiProfileTask -> Rate limit exceeded");
                    break;
                } else if (e.getStatusCode() == 401) { // 401表示Unauthorized, 该用户设置了隐私权限
                    this.logger.error("Exception in TwiProfileTask -> Unauthorized");
                } else if(e.getStatusCode() == 403){ // 403表示api更新了
                    this.logger.error("Exception in TwiProfileTask -> Forbidden");
                } else if (e.getStatusCode() == 503){ // 503表示twitter服务器吃不消了
                    this.logger.error("Exception in TwiFolloweeTask -> crawl:Followees");
                } else {
                    this.logger.error("Exception in TwiProfileTask -> crawl:profile");
                    e.printStackTrace();
                }
            }
        }
        if (!twiProfileEntity.isEmpty()) {
            this.writeDataQueue.add(twiProfileEntity);
        }
    }

    private void init() {
        this.uids = new ArrayList<>();

        /*
        一次性读出来的好处是减少了线程切换的时间
         */
        synchronized (this) {
            for (int i = 0; i < this.rate; i++) {
                if (this.readDataQueue.isEmpty()) {
                    break;
                }
                this.uids.add((Long) this.readDataQueue.poll());
            }
        }
        this.logger.info("TwiProfileTask.crawl number of uid:" + uids.size());
    }
}
