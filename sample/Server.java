/* Sample code for basic Server */

import java.rmi.ConnectException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.NoSuchElementException;

public class Server extends UnicastRemoteObject
        implements ServerIntf  {
	private static final int MASTER = 1;
	private static final Boolean FORWARDER = true;
	private static final Boolean PROCESSOR = false;
//	private static HashMap<Integer, Boolean> AllServerList;
	private static ArrayList<Integer> appServerList;
	private static ArrayList<Integer> forServerList;
	private static ServerIntf masterIntf;
	private static ServerIntf appIntf;
	private static Server server;
	private static int selfRPCPort;
	private static boolean selfRole;
	private static String selfIP;
	private static ServerLib SL;
	private static int curRound = 0;
	private static int numOfApps = 3;
	protected Server() throws RemoteException {
	}

	public static void main ( String args[] ) throws Exception {
		if (args.length != 3) throw new Exception("Need 3 args: <cloud_ip> <cloud_port>");
		selfIP = args[0];
		int basePort = Integer.parseInt(args[1]);
        int vmId = Integer.parseInt(args[2]);
		selfRPCPort = basePort + vmId;
		System.out.println("selfPRPCPort:" + selfRPCPort);
		System.out.println("---vmID----" + vmId);

        SL = new ServerLib(args[0], basePort);

		LocateRegistry.createRegistry(selfRPCPort);
		server = new Server();
		Naming.rebind(String.format("//%s:%d/server", selfIP, selfRPCPort), server);
//		float curTime = SL.getTime();
//		System.out.println("Current time is " + curTime);
//		curRound = vmId % numOfApps;

		if (vmId == MASTER){
			selfRole = FORWARDER;
			appServerList = new ArrayList<>();
			forServerList = new ArrayList<>();
			// provision machines according to current time
			// current master is not responsible for front work.

//			float curTime = SL.getTime();
//			int num = getVMNumber(curTime);
//			for (int i = 0; i < num-1; ++i){
//				SL.startVM();
//			}


			// launch 3 appServer
			for (int i = 0; i < 3; ++i){
				System.out.println("launching apps..");
				appServerList.add(SL.startVM() + basePort);
			}

//			Thread.sleep(4500);
			// launch 1 Forward
			for (int i = 0; i < 2; ++i){
				System.out.println("launching fors..");
				forServerList.add(SL.startVM() + basePort);
			}
		} else { // non-masetr // ask for role.
			try {
				masterIntf = (ServerIntf) Naming.lookup(String.format("//%s:%d/server", selfIP, basePort +1));
			} catch (NotBoundException e) {
				e.printStackTrace();
			}
			Content reply = masterIntf.getRole(selfRPCPort);
            if ((selfRole = reply.role) == FORWARDER){
                appServerList = reply.appServerList;
            }
        }


		if (selfRole == FORWARDER){
			SL.register_frontend();
			while (true){
				Cloud.FrontEndOps.Request r = SL.getNextRequest();
				long timeConsumed = forwardReq(r);
				System.out.println("timeConsumed:" + timeConsumed);
			}
		} else { // processor
			while (true){

			}
		}
	}


	public static long forwardReq(Cloud.FrontEndOps.Request r) throws Exception{
		int RPCPort = appServerList.get(curRound++);
//		boolean retry = true;
		ServerIntf curAppIntf = null;
		while (true){
            try {
                curAppIntf = (ServerIntf) Naming.lookup(String.format("//%s:%d/server", selfIP, RPCPort));
                break;
//				retry = false;
			}catch (Exception e){
//				Thread.sleep(100);
//				retry = true;
				continue;
            }
		}
		long timeConsumed = curAppIntf.processReq(r);
		curRound %= numOfApps;
		return timeConsumed;
	}


	public ArrayList<Integer> getAppServerList() throws RemoteException{
		return appServerList;
	}

	public Content getRole(int RPCport) throws RemoteException{
		// todo: what if a valid port???
		if (appServerList.contains(RPCport)){
			return new Content(PROCESSOR);
		}
		return new Content(FORWARDER, appServerList);
	}

	public long processReq(Cloud.FrontEndOps.Request r) throws RemoteException{
		long start = System.currentTimeMillis();
		SL.processRequest(r);
		long timeConsumed = System.currentTimeMillis() - start;
		return timeConsumed;
	}

	/**
	 * get number of needed vm in curTime
	 * @param curTime current time
	 * @return number of needed vms
     */
	private static int getVMNumber(float curTime){
		if (curTime <= 7){
			return 2;
		}
		if (curTime <= 19){
			return 4;
		}
		return 6;
	}
}

