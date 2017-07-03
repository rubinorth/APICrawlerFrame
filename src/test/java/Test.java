import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

public class Test {
    public Test() {
    }

    public void testTwi() {
        ConfigurationBuilder cb = new ConfigurationBuilder();

        cb.setOAuthAccessToken("2351360630-u1IuVV5LjUQ0M11PIdppqaFRAg2D6fBdf5vrHhN");
        cb.setOAuthAccessTokenSecret("iz6zG9hjYlA9L5YUXYGpBeJClHX8BUXiGUQwf5tQTebn7");
        cb.setOAuthConsumerKey("Ioc4i0j9pLKY1OhzSNbj6xpQL");
        cb.setOAuthConsumerSecret("p4A73rFAvLq0vidvuCD7v1eIM9kInxJVfObfwwlxRyqJ2WENCS");
        Twitter twitter = new TwitterFactory(cb.build()).getInstance();

        try {
            System.out.println(twitter.showUser("AilanthusG").toString());

//            System.out.println(twitter.showUser(2351360630l).toString());

//            System.out.println(twitter.getFriendsIDs(3310404343l, -1).getIDs().length);

        } catch (TwitterException e) {
            System.out.println(e.getStatusCode());
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws InterruptedException {
        Test test = new Test();
        test.testTwi();
    }

}



