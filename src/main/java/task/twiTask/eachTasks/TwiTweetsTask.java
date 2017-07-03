package task.twiTask.eachTasks;

import entity.twiEntity.TwiTweetsEntity;
import task.SpecificTask;
import twitter4j.*;
import twitter4j.internal.logging.Logger;

import java.util.concurrent.BlockingQueue;

/**
 * Created by nizeyang on 2016/8/22.
 * 根据用户爬取tweets
 */
public class TwiTweetsTask implements SpecificTask {

    private final Logger logger = Logger.getLogger(this.getClass());
    private Twitter twitter;
    private int rate;
    private BlockingQueue<Object> readDataQueue;
    private BlockingQueue<Object> writeDataQueue;

    public TwiTweetsTask(Twitter t, int c, BlockingQueue<Object> readDataQueue, BlockingQueue<Object> writeDataQueue) {
        this.twitter = t;
        this.rate = c;
        this.readDataQueue = readDataQueue;
        this.writeDataQueue = writeDataQueue;
    }

    @Override
    public void crawl() {
        /*
        遍历uids列表，访问api，将获得的tweets放到TwiTweetsEntity中，再写入writeDataQueue中
         */
        TwiTweetsEntity twiTweetsEntity;
        Long uid;
        Paging page; //爬推文是从最新的开始爬的

        String tmp;
        Long maxId; //maxid设置后，可以爬取所有id小于maxid的推文。最新的推文的id最大
        int size = 0;
        boolean end = false;

        while (!this.readDataQueue.isEmpty()) {
            /*
            从readDataQueue中读取一个uid
             */
            tmp = (String) this.readDataQueue.poll();
            uid = Long.parseLong(tmp.split("&")[0]);
            maxId = Long.parseLong(tmp.split("&")[1]);

            twiTweetsEntity = new TwiTweetsEntity(uid);
            page = new Paging();
            page.count(200);
            page.setMaxId(maxId);
            try {
                do {
                    ResponseList<Status> status = this.twitter.getUserTimeline(uid, page);
                    size = status.size();
                    maxId = twiTweetsEntity.add(status);
                    page.setMaxId(maxId);
                } while (size > 1);
                twiTweetsEntity.setFinished(true);
                this.writeDataQueue.add(twiTweetsEntity);

            } catch (TwitterException e) {
                if(e.getStatusCode() == 429){ // 次数超限
                    end = true;
                    this.writeDataQueue.add(twiTweetsEntity);
                    this.logger.error(e.getMessage());
                } else if (e.getStatusCode() == 401) { // 401表示Unauthorized, 该用户设置了隐私权限
                    twiTweetsEntity.setForbidden(true);
                    this.writeDataQueue.add(twiTweetsEntity);
                    this.logger.error(e.getMessage() + uid);
                } else if (e.getStatusCode() == 403) { // 403表示api更新了
                    this.logger.error(e.getMessage());
                } else if (e.getStatusCode() == 503){ // 503表示twitter服务器吃不消了
                    this.logger.error(e.getMessage());
                } else {
                    this.logger.error("Exception in TwiTweetsTask -> crawl:Tweets");
                    e.printStackTrace();
                }
            }
            if (end) {
                break;
            }
            while(this.writeDataQueue.size() > 50){
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

    }

}
