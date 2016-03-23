/* Sample code for basic Server */

import java.rmi.ConnectException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

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
//	private static int numOfApps = 3; // 0  1  2  3  4  5  6  7  8  9 10 11 12 13 14 15 16 17 18 19 20 21 22  23
	private static byte[] appInitConf = {2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 6, 6, 6, 6};
	private static byte[] forInitConf = {1, 1, 1, 1, 1, 1, 2, 2, 4, 4, 4, 4, 2, 2, 2, 2, 4, 4, 4, 4, 6, 6, 6, 6};
	private static LinkedList<Long> stats;
	private static long lastScaleoutTime;
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
			stats = new LinkedList<>();
			// provision machines according to current time
			// current master is not responsible for front work.

			float curTime = SL.getTime();
			byte[] nums = getVMNumber(curTime);

			// launch 3 appServer
			for (int i = 0; i < nums[1]; ++i){
				System.out.println("launching apps..");
				appServerList.add(SL.startVM() + basePort);
			}

//			Thread.sleep(4500);
			// launch 1 Forward
			for (int i = 0; i < nums[0]; ++i){
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
			while (SL.getStatusVM(2) == Cloud.CloudOps.VMStatus.Booting){
				Cloud.FrontEndOps.Request r = SL.getNextRequest();
				SL.drop(r);
			}
			while (true){
				Cloud.FrontEndOps.Request r = SL.getNextRequest();
				long timeConsumed = forwardReq(r);
				System.out.println("timeConsumed:" + timeConsumed);


                if (vmId == 1){ // scale
					//scaleOut()
					System.out.println("length:" + SL.getQueueLength());
					stats.addLast(timeConsumed);
					if (stats.size() > 5){
						stats.removeFirst();
						OptionalDouble avgTime = stats.stream().mapToLong(p -> p).average();

						if ((System.currentTimeMillis() - lastScaleoutTime)/1000 > 60 &&
								avgTime.getAsDouble() > 300){
							System.out.println("Now Scale out.");
							scaleOut(1);
							lastScaleoutTime = System.currentTimeMillis();
						}
					}
				}

			}
		} else { // processor
			while (true){

			}
		}
	}

	public static long scaleOut(int num){
		return 0;
	}

    public void welcomeNewApp(int RPCPort) throws RemoteException{

    }

	public static long forwardReq(Cloud.FrontEndOps.Request r) throws Exception{
		int RPCPort = appServerList.get(curRound);
		ServerIntf curAppIntf = null;
		while (true){
            try {
                curAppIntf = (ServerIntf) Naming.lookup(String.format("//%s:%d/server", selfIP, RPCPort));
                break;
			}catch (Exception e){
//				Thread.sleep(100);
				continue;
            }
		}
		long timeConsumed = curAppIntf.processReq(r);
		curRound = (curRound + 1) % appServerList.size();
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
	private static byte[] getVMNumber(float curTime){
		byte[] ret = new byte[2];
		if (curTime <= 6){
			ret[0] = 2;
			ret[1] = 3;
		}else if (curTime <= 16){
			ret[0] = 3;
			ret[1] = 4;
		} else {
			ret[0] = 4;
			ret[1] = 10;
		}
		return ret;
	}
}

