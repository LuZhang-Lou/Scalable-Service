
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public class Server extends UnicastRemoteObject
        implements ServerIntf  {
	private static final int MASTER = 1;
	private static final Boolean FORWARDER = true;
	private static final Boolean PROCESSOR = false;
	private static ConcurrentHashMap<Integer, Boolean> futureAppServerList;
    private static ConcurrentHashMap<Integer, Boolean> futureForServerList;
    private static ArrayList<Integer> appServerList;
	private static List<Integer> forServerList;
    private static ConcurrentLinkedDeque<Cloud.FrontEndOps.Request> localReqQueue;
	private static ServerIntf masterIntf;
	private static int selfRPCPort;
    private static int basePort;
	private static boolean selfRole;
	private static String selfIP;
	private static ServerLib SL;
	private static int curRound = 0;
	private static long appLastScaleoutTime;
    private static long forLastScaleoutTime;
    private static long interval = 1000;
    private static final long APP_ADD_COOL_DOWN_INTERVAL = 10000;
    private static final long FOR_ADD_COOL_DOWN_INTERVAL = 20000;
    private static final long MAX_FORWARDER_NUM = 0;
    private static final long MAX_APP_NUM = 12;
    private static int startNum = 1;
    private static int startForNum = 1;
	protected Server() throws RemoteException {
	}

	public static void main ( String args[] ) throws Exception {
		if (args.length != 3) throw new Exception("Need 3 args: <cloud_ip> <cloud_port>");
		selfIP = args[0];
		basePort = Integer.parseInt(args[1]);
        int vmId = Integer.parseInt(args[2]);
		selfRPCPort = basePort + vmId;
		System.out.println("selfPRPCPort:" + selfRPCPort);
		System.out.println("---vmID----" + vmId);


        Registry registry = LocateRegistry.getRegistry(selfIP, basePort);
        registry.bind("//localhost/no"+selfRPCPort, new Server());
        SL = new ServerLib(args[0], basePort);

        if (vmId == MASTER){
            appServerList = new ArrayList<>();
            futureAppServerList = new ConcurrentHashMap<>();

            //launch first vm first.
            futureAppServerList.put(SL.startVM() + basePort, true);
            SL.register_frontend();

            // get # of start vm
            while (SL.getQueueLength() == 0);
            long time1 = System.currentTimeMillis();
            SL.dropHead();
            while (SL.getQueueLength() == 0);
            long time2 = System.currentTimeMillis();


            interval = time2-time1;
            System.out.println("time2-time1:" + interval);
            if (interval < 150){
                startNum = 6;
                startForNum = 0;
            }else if (interval < 300){
                startNum = 5;
                startForNum = 0;
            } else if (interval < 650){
                startNum = 4;
                startForNum = 0;
            } else if (interval < 2000){
                startNum = 2;
                startForNum = 0;
            } else {
                startNum = 1;
                startForNum = 0;
            }
            System.out.println("interval:" + interval + " start:"+startNum + " startFor:" + startForNum );

        }

        localReqQueue = new ConcurrentLinkedDeque<>();


        // launch other start vms.
		if (vmId == MASTER){
			selfRole = FORWARDER;
            forServerList = Collections.synchronizedList(new ArrayList<Integer>());

            futureForServerList = new ConcurrentHashMap<>();

			for (int i = 0; i < startNum-1; ++i){
                futureAppServerList.put(SL.startVM() + basePort, true);
			}
            appLastScaleoutTime = System.currentTimeMillis();

            // launch Forward servers
            for (int i = 0; i < startForNum; ++i){
				futureForServerList.put(SL.startVM() + basePort, true);
			}
            forLastScaleoutTime = System.currentTimeMillis();


		} else { // non-masetr // ask for role.
            while (true) {
                try {

                    Registry reg = LocateRegistry.getRegistry(selfIP, basePort);
                    masterIntf = (ServerIntf) reg.lookup("//localhost/no"+String.valueOf(basePort+1));
                    break;
                } catch (Exception e) {
                    // e.printStackTrace();
                    continue;
                }
            }
			Content reply = masterIntf.getRole(selfRPCPort);
            if ((selfRole = reply.role) == FORWARDER){
                System.out.println("==========Forwarder");
                appServerList = reply.appServerList;
            } else {
                interval = reply.interval;
                System.out.println("==========App, interval:" + interval);
            }
        }

		if (selfRole == FORWARDER){
            if (vmId != MASTER) {
                SL.register_frontend();
            }
            while (vmId == MASTER && SL.getStatusVM(2) == Cloud.CloudOps.VMStatus.Booting && appServerList.size() == 0){
                SL.dropHead();
            }
			while (true){
                if (vmId == MASTER){
                    while (SL.getQueueLength() > 3 ){
                        SL.dropHead();
                    }
                    Cloud.FrontEndOps.Request r = SL.getNextRequest();
                    if (r == null){
                        continue;
                    }
                    forwardReq2(r);

                    int queueLen = SL.getQueueLength();
                    if (queueLen > appServerList.size() * 2){
                        scaleOutFor(1);
                    }

                } else {
                    while (SL.getQueueLength() > 3 ){
                        SL.dropHead();
                    }
                    Cloud.FrontEndOps.Request r = SL.getNextRequest();
                    if (r == null){
                        continue;
                    }
                    forwardReq2(r);
                }
			}
		} else { // processor
            Cloud.FrontEndOps.Request curReq;
            long lastTime = System.currentTimeMillis();
			while (true){
                if (localReqQueue.size() > 1){
                    SL.processRequest(localReqQueue.poll());
                    if (interval < 600) {
                        Cloud.FrontEndOps.Request r = localReqQueue.poll();
                        SL.drop(r);
                    }
                    if (System.currentTimeMillis() - lastTime > APP_ADD_COOL_DOWN_INTERVAL) {
                        lastTime = System.currentTimeMillis();
                        int scaleUpNum = (int)((localReqQueue.size())/2);
                        System.out.println("asking for scale up app:" + scaleUpNum);
                        if (scaleUpNum !=0) {
                            masterIntf.scaleOutApp(scaleUpNum);
                        }
                    }
                }
                if ((curReq = localReqQueue.poll()) != null){
                    SL.processRequest(curReq);
                }


			}
		}
	}

	public void scaleOutApp(int num) throws Exception{
        System.out.println("Receiving scale out app request:" + num);
        int curNum = appServerList.size() + futureAppServerList.size();
        if (curNum <= MAX_APP_NUM &&
                System.currentTimeMillis() - appLastScaleoutTime > APP_ADD_COOL_DOWN_INTERVAL) {
            appLastScaleoutTime = System.currentTimeMillis();
            for (int i = 0; i < num && (curNum + i < MAX_APP_NUM); ++i) {
                System.out.println("Scale out App");
                futureAppServerList.put(SL.startVM() + basePort, true);
            }
        }else {
            System.out.println("scaleout refused. : interval:" + (System.currentTimeMillis() - appLastScaleoutTime));
        }
	}

    public static void scaleOutFor(int num) {
        System.out.println("Check whetther to scaleup forwarder");
        if (forServerList.size() + futureForServerList.size() <= MAX_FORWARDER_NUM &&
                System.currentTimeMillis() - forLastScaleoutTime > FOR_ADD_COOL_DOWN_INTERVAL) {
            System.out.println("Scalue out Forwarder");
            futureForServerList.put(SL.startVM() + basePort, true);
            forLastScaleoutTime = System.currentTimeMillis();
        }
    }

    public void welcomeNewApp(int rpcPort) throws RemoteException{
        appServerList.add(rpcPort);
    }


    public static void forwardReq2(Cloud.FrontEndOps.Request r) throws Exception{
        while (appServerList.size()==0){
            Thread.sleep(5);
//            System.out.println("appServerList.size()==0");
        }
        int curPort = appServerList.get(curRound);
        ServerIntf curAppIntf = null;
        while (true){
            try {
                Registry reg = LocateRegistry.getRegistry(selfIP, basePort);
                curAppIntf = (ServerIntf) reg.lookup("//localhost/no"+curPort);
                break;
            }catch (Exception e){
                e.printStackTrace();
                continue;
            }
        }
        curAppIntf.addToLocalQue(r);
        curRound = (curRound + 1) % appServerList.size();
    }

    public void addToLocalQue(Cloud.FrontEndOps.Request r) throws Exception{
        localReqQueue.add(r);
    }


	public Content getRole(Integer newRPCport) throws RemoteException{
        if (futureAppServerList.containsKey(newRPCport)) {
            System.out.println("RPCport:" + newRPCport + " is app`");
            try {
                futureAppServerList.remove(newRPCport);
                appServerList.add(newRPCport);
                for (int i = 0; i < forServerList.size(); ++i){
                    int rpcPort = forServerList.get(i);
                    Registry reg = LocateRegistry.getRegistry(selfIP, basePort);
                    ServerIntf curForIntf = (ServerIntf) reg.lookup("//localhost/no"+rpcPort);
                    curForIntf.welcomeNewApp(newRPCport);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally{
                System.out.println("give interval:" + interval);
                return new Content(PROCESSOR, interval);
            }
        }
        // forwarder
        System.out.println("RPCport:" + newRPCport + " is ford");
        futureForServerList.remove(newRPCport);
        forServerList.add(newRPCport);
		return new Content(FORWARDER, appServerList);
	}

	public void processReq(Cloud.FrontEndOps.Request r) throws RemoteException{
		SL.processRequest(r);
	}

}

