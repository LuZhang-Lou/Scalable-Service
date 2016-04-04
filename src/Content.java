import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by Lu on 3/21/16.
 */
public class Content implements Serializable{
    public boolean role;
    public long interval;
    public ArrayList<Integer> appServerList;
    Content(boolean role){
        this.role = role;
    }

    Content(boolean role, long interval){
        this.role = role;
        this.interval = interval;
    }
}
