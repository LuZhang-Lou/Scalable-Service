import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by Lu on 3/21/16.
 */

public interface ServerIntf extends Remote {
    public Content getRole(Integer vmID) throws RemoteException;
    public Cloud.FrontEndOps.Request getFromCentralizedQueue() throws RemoteException;
    public void addToCentralizedQueue(Cloud.FrontEndOps.Request r) throws RemoteException;

}


