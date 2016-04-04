import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Created by Lu on 3/21/16.
 */

public interface ServerIntf extends Remote {
<<<<<<< HEAD
    public Content getRole(Integer vmID) throws RemoteException;
    public Cloud.FrontEndOps.Request getFromCentralizedQueue() throws RemoteException;
    public void addToCentralizedQueue(Cloud.FrontEndOps.Request r) throws RemoteException;
    public void killMe(Integer vmId, boolean type) throws RemoteException;
    public void killYourself() throws RemoteException;

=======
    public Content getRole(Integer RPCport) throws RemoteException;
//    public ArrayList<Integer> getAppServerList() throws RemoteException;
    public void welcomeNewApp(Integer RPCPort) throws RemoteException;
    public void updateInterval(long RPCPort) throws RemoteException;
    public void addToLocalQue(Cloud.FrontEndOps.Request r) throws Exception;
    public void scaleOutApp(int num) throws Exception;
    public void scaleInApp(int num) throws Exception;
    public void goodbyeApp(Integer rpcPort) throws RemoteException;
>>>>>>> 8ad16dfd4f8f6622598778f39044bd8472376017
}


