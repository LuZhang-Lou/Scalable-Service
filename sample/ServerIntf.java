import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by Lu on 3/21/16.
 */

public interface ServerIntf extends Remote {
    public Content getRole(Integer RPCport) throws RemoteException;
//    public ArrayList<Integer> getAppServerList() throws RemoteException;
    public void welcomeNewApp(Integer RPCPort) throws RemoteException;
    public void updateInterval(long RPCPort) throws RemoteException;
    public void addToLocalQue(Cloud.FrontEndOps.Request r) throws Exception;
    public void scaleOutApp(int num) throws Exception;
    public void scaleInApp(int num) throws Exception;
    public void goodbyeApp(Integer rpcPort) throws RemoteException;
}


