package task.twiTask.eachTasks;

import entity.twiEntity.TwiFollowEntity;
import task.SpecificTask;
import twitter4j.IDs;
import twitter4j.internal.logging.Logger;
import twitter4j.Twitter;
import twitter4j.TwitterException;

import java.util.concurrent.BlockingQueue;

/**
 * Created by nizeyang on 2016/8/17.
 * 爬取twi用户followee
 */
public class TwiFolloweeTask implements SpecificTask {

    private final Logger logger = Logger.getLogger(this.getClass());
    private Twitter twitter;
    private int rate;
    private BlockingQueue<Object> readDataQueue;
    private BlockingQueue<Object> writeDataQueue;

    public TwiFolloweeTask(Twitter t, int r, BlockingQueue<Object> readDataQueue, BlockingQueue<Object> writeDataQueue) {
        this.twitter = t;
        this.rate = r;
        this.readDataQueue = readDataQueue;
        this.writeDataQueue = writeDataQueue;
    }

    @Override
    public void crawl() {
        /*
        遍历uids列表，访问api，将获得的followee关系放到TwiFollowEntity中，再写入writeDataQueue中
         */
        TwiFollowEntity twiFollowEntity = new TwiFollowEntity();
        Long uid;
        Long cursor; // 标记从哪一页开始爬（因为followee多的一轮可能爬不完）
        IDs iDs;

        String tmp;
        boolean end = false;
        int rate = -1;
        while(!this.readDataQueue.isEmpty()) {
            /*
            从readDataQueue中读取一个uid
             */
            tmp = (String) this.readDataQueue.poll();
            uid = Long.parseLong(tmp.split("&")[0]);
            cursor = Long.parseLong(tmp.split("&")[1]);
            
            try {
                do {
                    rate++;
                    if (rate >= this.rate) {
                        end = true;
                        break;
                    }
                    iDs = this.twitter.getFriendsIDs(uid, cursor, 5000);
                    cursor = iDs.getNextCursor();
                    twiFollowEntity.add(uid, iDs, cursor); //当访问api次数达到限制后，当前用户的followee仍有可能没爬完，所以要记下cursor
                } while (cursor != 0); //cursor为0时表示followee全爬完了

            } catch (TwitterException e) {
                if(e.getStatusCode() == 429){ // 次数超限
                    end = true;
                    this.logger.error(e.getMessage());
                } else if (e.getStatusCode() == 401) { // 401表示Unauthorized, 该用户设置了隐私权限
                    twiFollowEntity.addF(uid);
                    this.logger.error(e.getMessage() + uid);
                } else if (e.getStatusCode() == 403) { // 403表示api更新了
                    this.logger.error(e.getMessage());
                } else if (e.getStatusCode() == 503){ // 503表示twitter服务器吃不消了
                    this.logger.error(e.getMessage());
                } else if (e.getStatusCode() == 404){ // 404表示该用户被删除了
                    twiFollowEntity.addF(uid);
                    this.logger.error(e.getMessage());
                } else {
                    this.logger.error("Exception in TwiFolloweeTask -> crawl:Followees, uid:" + uid);
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

        if (!twiFollowEntity.isEmpty()) {
            this.writeDataQueue.add(twiFollowEntity);
        }
    }

}
