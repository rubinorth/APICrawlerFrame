package entity.twiEntity;

import twitter4j.User;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by nizeyang on 2016/9/12.
 */
public class TwiProfileEntity {
    private Map<Long, User> up = new HashMap(); // Map<用户id, 他的profile>

    public TwiProfileEntity(){}

    public void add(Long uid, User u){
        this.up.put(uid, u);
    }

    public Map<Long, User> getUp(){
        return this.up;
    }

    public boolean isEmpty() {
        return this.up.isEmpty();
    }

}
