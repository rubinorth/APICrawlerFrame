package entity.twiEntity;

import twitter4j.IDs;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by nizeyang on 2016/8/16.
 * 用户follower关系实体类
 */
public class TwiFollowEntity {
    private Map<Long, Set<Long>> edges = new HashMap<>(); //Map<用户id, Set<他的follower>>
    private Set<Long> forbiddenUser = new HashSet<>(); //设置了隐私权限的用户id
    private Map<Long, Long> uc = new HashMap<>(); //Map<用户id, cursor>，爬完的就是0，没爬完的就是下一个cursor

    public TwiFollowEntity() {
    }

    public void add(Long uid, IDs iDs, Long cursor) {
        if (!this.edges.containsKey(uid)) {
            this.edges.put(uid, new HashSet<>());
        }
        for (Long id : iDs.getIDs()) {
            this.edges.get(uid).add(id);
        }
        this.uc.put(uid, cursor);
    }

    public boolean isEmpty() {
        if(this.edges.isEmpty() && this.forbiddenUser.isEmpty()) {
            return true;
        } else {
            return false;
        }
    }

    public Set<Long> getOldUsers() {
        return this.edges.keySet();
    }

    public Set<Long> getNewUsers() {
        Set<Long> newUsers = new HashSet<>();
        for (Long uid : this.edges.keySet()) {
            newUsers.addAll(this.edges.get(uid).stream().collect(Collectors.toList()));
        }
        return newUsers;
    }

    public Map<Long, Set<Long>> getEdges() {
        return this.edges;
    }

    public void addF(Long uid) {
        this.forbiddenUser.add(uid);
    }

    public Set<Long> getF() {
        return this.forbiddenUser;
    }

    public Map<Long, Long> getUc() {
        return this.uc;
    }
}
