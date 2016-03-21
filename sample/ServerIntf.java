import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by Lu on 3/21/16.
 */

public interface ServerIntf extends Remote {
    public boolean getRole(int RPCport) throws RemoteException;
    public long processReq(Cloud.FrontEndOps.Request r) throws RemoteException;
    public ArrayList<Integer> getAppServerList() throws RemoteException;
}


