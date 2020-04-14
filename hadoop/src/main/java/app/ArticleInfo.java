package app;

public class ArticleInfo {
    private String articleid;
    /*private String diggcount;
    private String burycount;
    private String viewcount;
    private String commentcount;*/
    private String type;
    private String istop;
    private String status;
    private String domain;

    public ArticleInfo(){}

    public ArticleInfo(String articleid,String diggcount,String burycount,String viewcount,String commentcount,String type,String istop,String status){
        this.articleid=articleid;
        /*this.diggcount=diggcount;
        this.burycount=burycount;
        this.viewcount=viewcount;
        this.commentcount=commentcount;*/
        this.type=type;
        this.istop=istop;
        this.status=status;
    }

    public ArticleInfo(String articleid,String type,String istop,String status,String domain){
        this.articleid=articleid;
        /*this.diggcount=diggcount;
        this.burycount=burycount;
        this.viewcount=viewcount;
        this.commentcount=commentcount;*/
        this.type=type;
        this.istop=istop;
        this.status=status;
        this.domain=domain;
    }

    public void setArticleid(String articleid) {
        this.articleid = articleid;
    }

    public String getArticleid() {
        return articleid;
    }

    /*public void setDiggcount(String diggcount) {
        this.diggcount = diggcount;
    }

    public String getDiggcount() {
        return diggcount;
    }

    public void setBurycount(String burycount) {
        this.burycount = burycount;
    }

    public String getBurycount() {
        return burycount;
    }

    public void setViewcount(String viewcount) {
        this.viewcount = viewcount;
    }

    public String getViewcount() {
        return viewcount;
    }

    public void setCommentcount(String commentcount) {
        this.commentcount = commentcount;
    }

    public String getCommentcount() {
        return commentcount;
    }*/

    public void setType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public void setIstop(String istop) {
        this.istop = istop;
    }

    public String getIstop() {
        return istop;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getStatus() {
        return status;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }
    public String getDomain() {
        return domain;
    }
}
