import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by Lu on 3/21/16.
 */
public class Content implements Serializable{
    public boolean role;
    public ArrayList<Integer> appServerList;
    Content(boolean role, ArrayList<Integer> list){
        this.role = role;
        this.appServerList = list;
    }
    Content(boolean role){
        this.role = role;
    }
}
