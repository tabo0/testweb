import com.opensymphony.xwork2.ActionContext;

import java.util.ArrayList;
import java.util.List;
public class testAction {

    public String success() throws Exception {
        String t="dept2";
        hbaseoperation baseOperation = new hbaseoperation();
        baseOperation.initconnection();
        List<User> list = null ;
        list=baseOperation.findnopid("dept2");
        ActionContext.getContext().put("list", list);
        return "success";
    }
    public String find() throws Exception {
        String t="dept2";
        hbaseoperation baseOperation = new hbaseoperation();
        baseOperation.initconnection();
        List<User> list = null ;
        list=baseOperation.subdept("dept2","1");
        ActionContext.getContext().put("list", list);
        return "success";
    }
}
