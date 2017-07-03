package entity.twiEntity;


import twitter4j.IDs;
import twitter4j.ResponseList;
import twitter4j.Status;

import java.util.*;

/**
 * Created by nizeyang on 2016/8/22.
 */
public class TwiTweetsEntity {

    private Long uid; // 用户id
    private List<Status> status; // List<他的tweets>
    private Long maxId; //maxId
    private boolean isForbidden; //是否forbid
    private boolean isFinished; //是否爬完

    public TwiTweetsEntity(Long u){
        this.uid = u;
        this.status = new ArrayList<>();
        this.maxId = Long.MAX_VALUE - 1l;
        this.isForbidden = false;
        this.isFinished = false;
    }

    public long add(ResponseList<Status> status) {
        for(Status s: status){
            this.status.add(s);
            if(s.getId() < this.maxId){
                this.maxId = s.getId();
            }
        }
        return this.maxId;
    }

    public boolean isEmpty() {
        return this.status.isEmpty();
    }

    public Long getUid() {
        return this.uid;
    }

    public List<Status> getStatus(){
        return this.status;
    }

    public void setForbidden(boolean forbidden) {
        isForbidden = forbidden;
    }

    public void setFinished(boolean finished) {
        isFinished = finished;
    }

    public Long getMaxId(){
        return this.maxId;
    }

    public String getS(){
        if(this.isForbidden){
            return "forbid";
        }else{
            if(this.isFinished){
                return "done";
            }else{
                return "undone";
            }
        }
    }

}

//public class TwiTweetsEntity {
//
//    private Map<Long, ResponseList<Status>> ut = new HashMap(); // Map<用户id, Set<他的tweets>>
//    private Map<Long, Long> um = new HashMap<>(); //Map<用户id, maxId>
//    private Set<Long> finishUser = new HashSet<>(); //爬完的用户
//
//    public TwiTweetsEntity(){}
//
//    public long add(Long uid, ResponseList<Status> status) {
//        if(!this.ut.containsKey(uid)) {
//            this.ut.put(uid, status);
//        } else {
//            this.ut.get(uid).addAll(status);
//        }
//
//        long maxId = Long.MAX_VALUE;
//        for(Status s: status){
//            if(s.getId() < maxId){
//                maxId = s.getId();
//            }
//        }
//        this.um.put(uid, maxId);
//
//        return maxId;
//    }
//
//    public ResponseList<Status> getStatus(Long uid){
//        return this.ut.get(uid);
//    }
//
//    public boolean isEmpty() {
//        return this.ut.isEmpty();
//    }
//
//    public Set<Long> getOldUsers() {
//        return this.ut.keySet();
//    }
//
//    public Map<Long, ResponseList<Status>> getUt() {
//        return this.ut;
//    }
//
//    public Map<Long, Long> getUm() {
//        return this.um;
//    }
//
//    public void setFinishUser(Long uid){
//        this.finishUser.add(uid);
//    }
//    public Set<Long> getFinishUser(){
//        return this.finishUser;
//    }
//}