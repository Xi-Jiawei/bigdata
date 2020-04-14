package app;

public class UserBehavior {
    private String userid;
    private String behavior;
    private String articleid;
    private String behaviortime;

    public UserBehavior(){}

    public UserBehavior(String userid,String behavior,String articleid,String behaviortime){
        this.userid = userid;
        this.behavior=behavior;
        this.articleid=articleid;
        this.behaviortime=behaviortime;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }
    public String getUserid() {
        return this.userid;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }
    public String getBehavior() {
        return this.behavior;
    }

    public void setArticleid(String articleid) {
        this.articleid = articleid;
    }
    public String getArticleid() {
        return this.articleid;
    }

    public void setBehaviortime(String behaviortime) {
        this.behaviortime = behaviortime;
    }
    public String getBehaviortime() {
        return this.behaviortime;
    }
}