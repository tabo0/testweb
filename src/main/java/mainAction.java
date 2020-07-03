import com.opensymphony.xwork2.ActionContext;

import java.util.ArrayList;
import java.util.List;

public class mainAction {
    String id;
    String flag;
    String idA;
    String idB;

    public void setIdA(String idA) {
        this.idA = idA;
    }

    public void setIdB(String idB) {
        this.idB = idB;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public String getIdA() {
        return idA;
    }

    public String getIdB() {
        return idB;
    }

    public String getId() {
        return id;
    }

    public String getFlag() {
        return flag;
    }

    public String findfollow() throws Exception {
        String t="user1";
        opration opration = new opration();
        opration.initconnection();
        List<User> list = null ;
        list=opration.find_family(t,id,"follow");
        for(User u : list){
            u.init();
        }
        ActionContext.getContext().put("list", list);
        return "success";
    }
    public String AIsfollowB() throws Exception {
        String t="user1";
        opration opration = new opration();
        opration.initconnection();
        Boolean ans=opration.AisfollowB(t,idA,idB);
        List<User> list = new ArrayList<User>();
        User user= new User();
        if(ans==false) user.setRowKey(idA+"没有关注"+idB);
        else user.setRowKey(idA+"关注了"+idB);
        list.add(user);
        ActionContext.getContext().put("list", list);
        return "success";
    }
    public String fans() throws Exception {
        String t="user1";
        opration opration = new opration();
        opration.initconnection();
        List<User> list = null ;
        list=opration.find_family(t,id,"fans");
        for(User u : list){
            u.init();
        }
        ActionContext.getContext().put("list", list);
        return "success";
    }
    public String write() throws Exception {
        String t="user1";
        opration opration = new opration();
        opration.initconnection();
        List<User> list = null ;
        opration.AfollowB(t,idA,idB);
        return "success";
    }
    public String delete() throws Exception {
        String t="user1";
        opration opration = new opration();
        opration.initconnection();
        List<User> list = null ;
        opration.ADeletefollowB(t,idA,idB);
        return "success";
    }
}
