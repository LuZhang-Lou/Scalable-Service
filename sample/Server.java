/* Sample code for basic Server */

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Server extends UnicastRemoteObject
        implements ServerIntf  {
	private static final int MASTER = 1;
	private static final Boolean FORWARDER = true;
	private static final Boolean PROCESSOR = false;
	private static ConcurrentHashMap<Integer, Boolean> futureAppServerList;
    private static ConcurrentHashMap<Integer, Boolean> futureForServerList;
    private static ArrayList<Integer> appServerList;
	private static ArrayList<Integer> forServerList;
    private static ConcurrentLinkedQueue<Cloud.FrontEndOps.Request> localReqQueue;
	private static ServerIntf masterIntf;
	private static ServerIntf appIntf;
	private static Server server;
	private static int selfRPCPort;
    private static int basePort;
	private static boolean selfRole;
	private static String selfIP;
	private static ServerLib SL;
	private static int curRound = 0;
	private static LinkedList<Long> stats;
	private static long appLastScaleoutTime;
    private static long forLastScaleoutTime;
    private static final long APP_ADD_COOL_DOWN_INTERVAL = 5000;
    private static final long FOR_ADD_COOL_DOWN_INTERVAL = 20000;
    private static final long MAX_FORWARDER_NUM = 4;
    private static final long MAX_APP_NUM = 12;
    private static int startNum = 1;
    private static int startForNum = 1;
    private static long systemStartTime;
	protected Server() throws RemoteException {
	}

	public static void main ( String args[] ) throws Exception {
		if (args.length != 3) throw new Exception("Need 3 args: <cloud_ip> <cloud_port>");
        systemStartTime = System.currentTimeMillis();
		selfIP = args[0];
		basePort = Integer.parseInt(args[1]);
        int vmId = Integer.parseInt(args[2]);
		selfRPCPort = basePort + vmId;
		System.out.println("selfPRPCPort:" + selfRPCPort);
		System.out.println("---vmID----" + vmId);


        SL = new ServerLib(args[0], basePort);
        if (vmId == 1){
            System.out.println("launching apps..");
            appServerList = new ArrayList<>();
            futureAppServerList = new ArrayList<>();
//            appServerList.add(SL.startVM() + basePort);
            futureAppServerList.add(SL.startVM() + basePort);
            SL.register_frontend();
            while (SL.getQueueLength() == 0);
            long time1 = System.currentTimeMillis();
            SL.dropHead();
            while (SL.getQueueLength() == 0);
            long time2 = System.currentTimeMillis();
            long interval = time2-time1;
            System.out.println("time2-time1:" + interval);
            if (interval < 150){
                startNum = 6;
                startForNum = 2;
            }else if (interval < 300){
                startNum = 5;
                startForNum = 1;
            } else if (interval < 650){
                startNum = 4;
                startForNum = 1;
            } else if (interval < 2000){
                startNum = 2;
                startForNum = 0;
            } else {
                startNum = 1;
                startForNum = 0;
            }

            System.out.println("interval:" + interval + " start:"+startNum);
        }

        localReqQueue = new ConcurrentLinkedQueue<>();
		LocateRegistry.createRegistry(selfRPCPort);
		server = new Server();
		Naming.rebind(String.format("//%s:%d/server", selfIP, selfRPCPort), server);

		if (vmId == MASTER){
			selfRole = FORWARDER;
			forServerList = new ArrayList<>();
            futureForServerList = new ArrayList<>();
			stats = new LinkedList<>();

			for (int i = 0; i < startNum-1; ++i){
				System.out.println("launching apps..");
				SL.startVM();
			}
            appLastScaleoutTime = System.currentTimeMillis();

//			 launch 1 Forward
			for (int i = 0; i < 1; ++i){
				System.out.println("launching fors..");
				futureForServerList.add(SL.startVM() + basePort);
			}
            forLastScaleoutTime = System.currentTimeMillis();


		} else { // non-masetr // ask for role.
            while (true) {
                try {
                    masterIntf = (ServerIntf) Naming.lookup(String.format("//%s:%d/server", selfIP, basePort + 1));
                    break;
                } catch (Exception e) {
                    e.printStackTrace();
                    continue;
                }
            }
			Content reply = masterIntf.getRole(selfRPCPort);
            if ((selfRole = reply.role) == FORWARDER){
                System.out.println("==========Forwarder");
                appServerList = reply.appServerList;
            } else {
                System.out.println("==========App");
            }
        }

		if (selfRole == FORWARDER){
            if (vmId != 1) {
                SL.register_frontend();
            }
//			while (SL.getStatusVM(2) == Cloud.CloudOps.VMStatus.Booting){
            while (SL.getStatusVM(2) == Cloud.CloudOps.VMStatus.Booting && appServerList.size() == 0){
				Cloud.FrontEndOps.Request r = SL.getNextRequest();
                System.out.println("drop here");
                SL.drop(r);
			}
			while (true){
                int globalQueueLen = SL.getQueueLength();
                while (appServerList.size() != 0 && globalQueueLen > appServerList.size() * 2){
                    System.out.println("drop on forwarder:" + globalQueueLen);
                    SL.dropHead();
                    globalQueueLen--;
                }
				Cloud.FrontEndOps.Request r = SL.getNextRequest();
                forwardReq2(r);
//                System.out.println("forwarder joins..");

                if (vmId == 1){
                    int queueLen = SL.getQueueLength();
                    System.out.println("1:global ql:" + queueLen);
                    if (queueLen != 0){
                        System.out.println("global ql:" + queueLen);
                    }
                    if (queueLen > appServerList.size() * 2){
                        scaleOutFor(1);
                    }
                }
			}
		} else { // processor
            Cloud.FrontEndOps.Request curReq;
            long lastTime = System.currentTimeMillis();
			while (true){
//                System.out.println("local ReqQueue.size:" + localReqQueue.size());
                while (localReqQueue.size() > 1){
                    System.out.println("localReqQueue.size:"+localReqQueue.size());
                    if (System.currentTimeMillis() - lastTime > APP_ADD_COOL_DOWN_INTERVAL) {
                        lastTime = System.currentTimeMillis();
                        int scaleUpNum = (int)((localReqQueue.size())/3);
                        System.out.println("asking for scale up app:" + scaleUpNum);
                        if (scaleUpNum !=0) {
                            masterIntf.scaleOutApp(scaleUpNum);
                        }
                    }
                    System.out.println("drop on app");
                    SL.drop(localReqQueue.poll());
                }
                if ((curReq = localReqQueue.poll()) != null){
                    System.out.println("join battle...");
                    SL.processRequest(curReq);
                }
			}
		}
	}

    // todo: set a scaleup flog, return instantly.
	public void scaleOutApp(int num) throws Exception{
        System.out.println("Receiveing scale up app request:" + num);
        int curNum = appServerList.size() + futureAppServerList.size();
        if (curNum <= MAX_APP_NUM &&
                System.currentTimeMillis() - appLastScaleoutTime > APP_ADD_COOL_DOWN_INTERVAL) {
            System.out.println("scaleup accpeted. : interval:" + (System.currentTimeMillis() - appLastScaleoutTime));
            appLastScaleoutTime = System.currentTimeMillis();
            for (int i = 0; i < num && (curNum + i < MAX_APP_NUM); ++i) {
                System.out.println("Scale up App");
                futureAppServerList.add(SL.startVM() + basePort);
            }
        }else {
            System.out.println("scaleup refused. : interval:" + (System.currentTimeMillis() - appLastScaleoutTime));
        }
	}

    public static void scaleOutFor(int num) {
        System.out.println("Check whetther to scaleup forwarder");
        if (forServerList.size() + futureForServerList.size() <= MAX_FORWARDER_NUM &&
                System.currentTimeMillis() - forLastScaleoutTime > FOR_ADD_COOL_DOWN_INTERVAL) {
            System.out.println("Scalue up Forwarder");
            futureForServerList.add(SL.startVM() + basePort);
            forLastScaleoutTime = System.currentTimeMillis();
        }
    }

    public void welcomeNewApp(int rpcPort) throws RemoteException{
        appServerList.add(rpcPort);
    }


    public static void forwardReq2(Cloud.FrontEndOps.Request r) throws Exception{
        int RPCPort = appServerList.get(curRound);
        ServerIntf curAppIntf = null;
        while (true){
            try {
                curAppIntf = (ServerIntf) Naming.lookup(String.format("//%s:%d/server", selfIP, RPCPort));
                break;
            }catch (Exception e){
                e.printStackTrace();
                continue;
            }
        }
//        System.out.println("send to:" + curRound);
        curAppIntf.addToLocalQue(r);
        curRound = (curRound + 1) % appServerList.size();
    }

    public void addToLocalQue(Cloud.FrontEndOps.Request r) throws Exception{
        localReqQueue.add(r);
//        System.out.println("localReqQueue.size:"+localReqQueue.size());
    }


	public Content getRole(Integer RPCport) throws RemoteException{
        if (futureAppServerList.contains(RPCport)) {
            System.out.println("RPCport:" + RPCport + " is app`");
            try {
                futureAppServerList.remove(RPCport);
                for (int rpcPort : forServerList) {
                    ServerIntf curForIntf = (ServerIntf) Naming.lookup(String.format("//%s:%d/server", selfIP, rpcPort));
                    curForIntf.welcomeNewApp(rpcPort);
                }
                appServerList.add(RPCport);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return new Content(PROCESSOR);
        }
        // forwarder
        System.out.println("RPCport:" + RPCport + " is ford");
        futureForServerList.remove(RPCport);
        forServerList.add(RPCport);
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
        // ret[0]: forServer,  ret[1]: appServer
        ret[0] = 1;
        ret[1] = 1;

        /*
		if (curTime <= 6){
			ret[0] = 2;
			ret[1] = 3;
		}else if (curTime <= 16){
			ret[0] = 3;
			ret[1] = 4;
		} else {
			ret[0] = 4;
			ret[1] = 6;
		}
		*/
		return ret;
	}
}

