package task.twiTask;

import task.SpecificTask;
import task.GeneralTask;
import task.twiTask.eachTasks.*;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.ConfigurationBuilder;

import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

/**
 * Created by nizeyang on 2016/7/19.
 * 爬取twitter数据的线程
 */
public class TwiTask implements GeneralTask {

    private final Logger logger = Logger.getLogger("TwiTask");
    private String whichApi;

    private int taskID;
    private String consumerKey;
    private String consumerSecret;
    private String accessToken;
    private String accessTokenSecret;


    private BlockingQueue<Object> readDataQueue;
    private BlockingQueue<Object> writeDataQueue;

    private Twitter twitter;

    private SpecificTask specificTask;

    public TwiTask(int i, String consumerKey, String consumerSecret,
                   String accessToken, String accessTokenSecret,
                   BlockingQueue<Object> readDataQueue, BlockingQueue<Object> writeDataQueue,
                   String whichApi) {
        this.taskID = i;
        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.accessToken = accessToken;
        this.accessTokenSecret = accessTokenSecret;

        this.readDataQueue = readDataQueue;
        this.writeDataQueue = writeDataQueue;

        this.whichApi = whichApi;

        this.init();
    }


    @Override
    public void run() {
        this.specificTask.crawl();
        this.logger.info("TwiTask : (" + this.taskID + ") finished\n");
    }


    private void init() {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        int rate = 0; //api每个时间段的访问次数限制

        switch (this.whichApi){
            case "followee": {
                cb.setOAuthAccessToken(this.accessToken);
                cb.setOAuthAccessTokenSecret(this.accessTokenSecret);
                cb.setOAuthConsumerKey(this.consumerKey);
                cb.setOAuthConsumerSecret(this.consumerSecret);
                OAuthAuthorization auth = new OAuthAuthorization(cb.build());
                this.twitter = new TwitterFactory().getInstance(auth);
                rate = 14;
                break;
            }
            case "profile": {
                cb.setOAuthAccessToken(this.accessToken);
                cb.setOAuthAccessTokenSecret(this.accessTokenSecret);
                cb.setOAuthConsumerKey(this.consumerKey);
                cb.setOAuthConsumerSecret(this.consumerSecret);
                OAuthAuthorization auth = new OAuthAuthorization(cb.build());
                this.twitter = new TwitterFactory().getInstance(auth);
                rate = 179;
                break;
            }
            case "tweet": {
                cb.setApplicationOnlyAuthEnabled(true);
                this.twitter = new TwitterFactory(cb.build()).getInstance();
                this.twitter.setOAuthConsumer(this.consumerKey, this.consumerSecret);
                try {
                    this.twitter.getOAuth2Token();
                } catch (TwitterException e) {
                    e.printStackTrace();
                }
                rate = 299;
                break;
            }
            default:
                break;
        }

        switch (this.whichApi){
            case "followee": {
                this.specificTask = new TwiFolloweeTask(this.twitter, rate, this.readDataQueue, this.writeDataQueue);
                break;
            }
            case "profile": {
                this.specificTask = new TwiProfileTask(this.twitter, rate, this.readDataQueue, this.writeDataQueue);
                break;
            }
            case "tweet": {
                this.specificTask = new TwiTweetsTask(this.twitter, rate, this.readDataQueue, this.writeDataQueue);
                break;
            }
            default:
                break;
        }

        this.logger.info("TwiTask start: (" + this.taskID + ") \n" +
                this.consumerKey + " " + this.consumerSecret + " " +
                this.accessToken + " " + this.accessTokenSecret + " rate=" + rate + "\n");
    }

}
