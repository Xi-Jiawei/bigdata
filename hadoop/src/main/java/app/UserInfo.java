package app;

public class UserInfo {
    private String userid;
    private String gender;
    private String status;
    /*private String follownum;
    private String fansnum;
    private String friendnum;*/
    private String level;
    private String dgreee;
    private String school;
    private String major;
    private String skill;
    private String interest;

    public UserInfo(){}

    public UserInfo(String userid,String gender,String status,String follownum,String fansnum,String friendnum){
        this.userid = userid;
        this.gender=gender;
        this.status=status;
        /*this.follownum=follownum;
        this.fansnum=fansnum;
        this.friendnum=friendnum;*/
    }

    public UserInfo(String userid,String gender,String status,String level,String dgreee,String school,String major,String skill,String interest){
        this.userid = userid;
        this.gender=gender;
        this.status=status;
        this.level=level;
        this.dgreee=dgreee;
        this.school=school;
        this.major=major;
        this.skill=skill;
        this.interest=interest;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }
    public String getUserid() {
        return this.userid;
    }

    public void setGender(String behavior) {
        this.gender=gender;
    }
    public String getGender() {
        return this.gender;
    }

    public void setStatus(String articleid) {
        this.status=status;
    }
    public String getStatus() {
        return this.status;
    }

    /*public void setFollownum(String behaviortime) {
        this.follownum=follownum;
    }
    public String getFollownum() {
        return this.follownum;
    }

    public void setFansnum(String fansnum) {
        this.fansnum = fansnum;
    }
    public String getFansnum() {
        return fansnum;
    }

    public void setFriendnum(String friendnum) {
        this.friendnum = friendnum;
    }
    public String getFriendnum() {
        return friendnum;
    }*/

    public void setLevel(String level) {
        this.level = level;
    }
    public String getLevel() {
        return level;
    }

    public void setDgreee(String dgreee) {
        this.dgreee = dgreee;
    }
    public String getDgreee() {
        return dgreee;
    }

    public void setSchool(String school) {
        this.school = school;
    }
    public String getSchool() {
        return school;
    }

    public void setMajor(String major) {
        this.major = major;
    }
    public String getMajor() {
        return major;
    }

    public void setSkill(String skill) {
        this.skill = skill;
    }
    public String getSkill() {
        return skill;
    }

    public void setInterest(String interest) {
        this.interest = interest;
    }
    public String getInterest() {
        return interest;
    }
}
