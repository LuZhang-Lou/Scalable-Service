import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by Lu on 3/21/16.
 */

public interface ServerIntf extends Remote {
    public Content getRole(int RPCport) throws RemoteException;
    public long processReq(Cloud.FrontEndOps.Request r) throws RemoteException;
    public ArrayList<Integer> getAppServerList() throws RemoteException;
    public void welcomeNewApp(int RPCPort) throws RemoteException;
    public void addToLocalQue(Cloud.FrontEndOps.Request r) throws Exception;
}


