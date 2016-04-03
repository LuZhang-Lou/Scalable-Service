
import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public class Server extends UnicastRemoteObject
        implements ServerIntf, Cloud.DatabaseOps  {
	private static final int MASTER = 1;
    private static final int CACHE = 100;
//    private static final int CACHE = 2;
	private static final Boolean FORWARDER = true;
	private static final Boolean PROCESSOR = false;
	private static ConcurrentHashMap<Integer, Boolean> futureAppServerList;
    private static ConcurrentHashMap<Integer, Boolean> futureForServerList;
    private static ArrayList<Integer> appServerList;
	private static List<Integer> forServerList;
//    private static ConcurrentLinkedDeque<Cloud.FrontEndOps.Request> localReqQueue;
    private static ConcurrentLinkedDeque<WrapperReq> centralizedQueue;
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
    private static final long APP_ADD_COOL_DOWN_INTERVAL = 60000;
    private static final long FOR_ADD_COOL_DOWN_INTERVAL = 20000;
    private static final long MAX_FORWARDER_NUM = 1;
    private static final long MAX_APP_NUM = 10;
    private static int startNum = 1;
    private static int startForNum = 0;
    private static int newAppNum = 0;
    private static final int UPDATE_INTERVAL_HIT = 3;


    // especially for cache
    private static LRUCache<String, String> cache;
    private static Cloud.DatabaseOps DB = null;
    private static Cloud.DatabaseOps cacheIntf = null;


    //todo: add locks

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

        if (vmId == CACHE){
            cache = new LRUCache<String, String>(512);
            DB = SL.getDB();

        }else {
            if (vmId == MASTER) {
                appServerList = new ArrayList<>();
                futureAppServerList = new ConcurrentHashMap<>();
                forServerList = Collections.synchronizedList(new ArrayList<Integer>());
                centralizedQueue = new ConcurrentLinkedDeque<>();

                cache = new LRUCache<String, String>(512);
                DB = SL.getDB();

                //launch first vm first.
                futureAppServerList.put(SL.startVM() + basePort, true);
                SL.register_frontend();

                // get # of start vm
                while (SL.getQueueLength() == 0) ;
                long time1 = System.currentTimeMillis();
                SL.dropHead();
                while (SL.getQueueLength() == 0) ;
                long time2 = System.currentTimeMillis();

                interval = time2 - time1;
                System.out.println("time2-time1:" + interval);
                if (interval < 130) {
                    startNum = 10;
                    startForNum = 1;
                } else if (interval < 150) {
                    startNum = 7;
                    startForNum = 1;
                } else if (interval < 300) {
                    startNum = 5;
                    startForNum = 0;
                } else if (interval < 650) {
                    startNum = 4;
                    startForNum = 0;
                } else {
                    startNum = 1;
                    startForNum = 0;
                }
                System.out.println("interval:" + interval + " start:" + startNum + " startFor:" + startForNum);
            }

//            localReqQueue = new ConcurrentLinkedDeque<>();

            // launch other start vms.
            if (vmId == MASTER) {
                selfRole = FORWARDER;
                futureForServerList = new ConcurrentHashMap<>();

                for (int i = 0; i < Math.min(4, startNum) ; ++i) {
                    futureAppServerList.put(SL.startVM() + basePort, true);
                }

                // launch Forward servers
                for (int i = 0; i < startForNum; ++i) {
                    futureForServerList.put(SL.startVM() + basePort, true);
                }
                forLastScaleoutTime = System.currentTimeMillis();

                for (int i = 4; i < startNum - 1; ++i) {
                    futureAppServerList.put(SL.startVM() + basePort, true);
                }
                appLastScaleoutTime = 0;


            } else { // non-masetr
                // ask for role.
                while (true) {
                    try {
                        Registry reg = LocateRegistry.getRegistry(selfIP, basePort);
                        masterIntf = (ServerIntf) reg.lookup("//localhost/no" + String.valueOf(basePort + 1));
                        break;
                    } catch (Exception e) {
                        // e.printStackTrace();
                        continue;
                    }
                }

                // if cannot reach to master, master died...
                Content reply = null;
                try {
                    reply = masterIntf.getRole(selfRPCPort);
                } catch (Exception e){
                    e.printStackTrace();
                    return;
                }
                if ((selfRole = reply.role) == FORWARDER) {
                    System.out.println("==========Forwarder");
//                    appServerList = reply.appServerList;
                } else {
                    interval = reply.interval;
                    System.out.println("==========App, interval:" + interval);
                }
            }

            if (selfRole == FORWARDER) {
                if (vmId != MASTER) {
                    SL.register_frontend();
                }
                while (vmId == MASTER && appServerList.size() == 0) {
                    SL.dropHead();
                    System.out.println("DropHead");
                    Thread.sleep(100);
                }
                int dropBCCongestion  = 0;
                while (true) {
                    if (vmId == MASTER) {
                        if (newAppNum >= UPDATE_INTERVAL_HIT){
                            if (interval > 200){
                                broadcastInterval(200);
                            }
                            newAppNum = 0;
                        }

                        while (SL.getQueueLength() > 4) {
                            SL.dropHead();
                            dropBCCongestion++;
                            System.out.println("drop b.c. congestion:" + dropBCCongestion);
                        }
                        Cloud.FrontEndOps.Request r = SL.getNextRequest();
                        long ts = System.currentTimeMillis();
                        if (r == null) {
                            continue;
                        }
                        centralizedQueue.add(new WrapperReq(r, ts));

                        while (centralizedQueue.size() >= appServerList.size()*1.5){
                            WrapperReq cur = centralizedQueue.poll();
//                            if (cur != null || cur.isTimeout()) {
                            if (cur != null) {
                                SL.drop(cur.request);
                                dropBCCongestion++;
                                System.out.println("drop b.c. mastercongestion:" + dropBCCongestion);
                            }
                        }

//                        int queueLen = SL.getQueueLength();
//                        if (queueLen > appServerList.size() * 1) {
//                            scaleOutFor(1);
//                            scaleOutApp(1);
//                        }
                    } else { // normal forwarder
                        while (SL.getQueueLength() > 3 ) {
                            SL.dropHead();
                            dropBCCongestion++;
                            System.out.println("drop b.c. congestion:" + dropBCCongestion);
                        }
                        Cloud.FrontEndOps.Request r = SL.getNextRequest();
                        long ts = System.currentTimeMillis();
                        if (r == null) {
                            continue;
                        }
                        masterIntf.addToCentralizedQueue(new WrapperReq(r, ts));

                    }
                }
            } else { // processor
                ////////////////////////////////////////////////////////////////////////////
                ////////////////////////////////////PROCESS/////////////////////////////////
                ///////////////////////////////////////////////////////////////////////////

                long lastTime = 0;
                cacheIntf = (Cloud.DatabaseOps) masterIntf;
                long lastScaleInTime = 0;
                while (true) {
                    WrapperReq r = null;
                    try {
                        r = masterIntf.getFromCentralizedQueue();
                    }catch (java.rmi.UnmarshalException | java.rmi.ConnectException | java.rmi.ConnectIOException e){
                        continue;
                    }
                    SL.processRequest(r.request, cacheIntf);
                    /*
                    if (r.isTimeout()){
                        SL.drop(r.request);
                        System.out.println("ClientDrop3");
                    }
                    else {
                        SL.processRequest(r.request, cacheIntf);
//                        System.out.println("process");
                    }
                    */

                }
            }
        }
	}

    public void addToCentralizedQueue (WrapperReq r) throws RemoteException{
        centralizedQueue.add(r);
    }

    public WrapperReq getFromCentralizedQueue() throws RemoteException{
        WrapperReq r = null;
//        while (centralizedQueue.size() > appServerList.size()*1.8){
//            r = centralizedQueue.poll();
//            if (r!=null || r.isTimeout()){
//            if (r.isTimeout()){
//                SL.drop(r.request);
//                System.out.println("ClientDrop1");
//            } else {
//                break;
//            }
//        }
        while (true){
            r = centralizedQueue.poll();
            if (r == null){
                continue;
            }else {
                break;
            }
//            if (r.isTimeout()){
//                System.out.println("ClientDrop2");
//                SL.drop(r.request);
//                continue;
//            }
//            break;
        }
        return r;
    }


	public void scaleOutApp(int num) throws Exception{
        System.out.println("Ready to scale out app:" + num);
        int curNum = appServerList.size() + futureAppServerList.size();
        num = Math.min(num, 4);
        if (curNum <= MAX_APP_NUM &&
                System.currentTimeMillis() - appLastScaleoutTime > APP_ADD_COOL_DOWN_INTERVAL) {
            appLastScaleoutTime = System.currentTimeMillis();
            for (int i = 0; i < num && (curNum + i < MAX_APP_NUM); ++i) {
                System.out.println("Scale out App");
                futureAppServerList.put(SL.startVM() + basePort, true);
                newAppNum++;
            }
        }else {
            System.out.println("scaleout refused. : interval:" + (System.currentTimeMillis() - appLastScaleoutTime));
        }
	}

    public static void scaleOutFor(int num) {
        System.out.println("Check whetther to scaleup forwarder");

        int sum = forServerList.size() + futureForServerList.size();
        System.out.println("forwardersize" + sum);

        if (forServerList.size() + futureForServerList.size() < MAX_FORWARDER_NUM &&
                System.currentTimeMillis() - forLastScaleoutTime > FOR_ADD_COOL_DOWN_INTERVAL) {
            System.out.println("Scale out Forwarder");
            futureForServerList.put(SL.startVM() + basePort, true);
            forLastScaleoutTime = System.currentTimeMillis();
        }
    }

    public void welcomeNewApp(int rpcPort) throws RemoteException{
        appServerList.add(rpcPort);
    }

    public void updateInterval (long newInterval) throws RemoteException{
        this.interval =  newInterval;
        System.out.println("update interval to " + newInterval);
    }


    /*
    public static void forwardReq(Cloud.FrontEndOps.Request r) throws Exception{
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
    */

//    public void addToLocalQue(Cloud.FrontEndOps.Request r) throws Exception{
//        localReqQueue.add(r);
//    }


	public Content getRole(Integer newRPCport) throws RemoteException{
        if (futureAppServerList.containsKey(newRPCport)) {
            System.out.println("RPCport:" + newRPCport + " is app`");
            try {
                futureAppServerList.remove(newRPCport);
                appServerList.add(newRPCport);
                /*
                for (int i = 0; i < forServerList.size(); ++i){
                    int rpcPort = forServerList.get(i);
                    Registry reg = LocateRegistry.getRegistry(selfIP, basePort);
                    ServerIntf curForIntf = (ServerIntf) reg.lookup("//localhost/no"+rpcPort);
                    curForIntf.welcomeNewApp(newRPCport);
                }*/
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
        return new Content(FORWARDER);
    }

    public static void broadcastInterval(long newInterval) throws Exception {
        for (int i = 0; i < appServerList.size(); ++i){
            int rpcPort = appServerList.get(i);
            Registry reg = LocateRegistry.getRegistry(selfIP, basePort);
            ServerIntf curForIntf = (ServerIntf) reg.lookup("//localhost/no"+rpcPort);
            curForIntf.updateInterval(newInterval);
        }
    }


    public synchronized String get(String key) throws RemoteException {

        String trimmedKey = key.trim();
        if (cache.containsKey(trimmedKey)){
            String value = cache.get(trimmedKey);
            return value;
        } else{
            String value = DB.get(trimmedKey);
            cache.put(trimmedKey, value);
            return value;
        }
    }

    public synchronized boolean set(String key, String value, String password) throws RemoteException {
//        return DB.set(key, value, password);

//         write through
        System.out.println("CallSet:" + password);
        boolean ret = DB.set(key, value, password);
//         if success, insert in cache
        if (ret){
            cache.put(key, value);
            return true;
        }
        return false;
    }


//    public synchronized boolean set(String key, String value, String password) throws RemoteException {
//        return DB.set(key, value, password);

//        if(!password.equals("sqwe")){
//            return false;
//        } else {
//            cache.put(key, value);
//            return true;
//        }
//    }


    public synchronized boolean transaction(String item, float price, int qty) throws RemoteException {
        boolean ret = DB.transaction(item, price, qty);
        if (ret) {
//             update: add change to cache
//            cache.remove(item);
            String trimmedItem = item.trim();
            String qtyStr = trimmedItem + "_qty";
            cache.put(qtyStr, String.valueOf(Integer.parseInt(cache.get(qtyStr))-qty));
        }
        System.out.println("purchase: " + item +" qty:" + qty + "ret:" + ret);
        return ret;
    }


//    public synchronized boolean transaction(String item, float price, int qty) throws RemoteException {
//        String trimmedItem = item.trim();
//        String value = cache.get(trimmedItem);
//        if(value != null && value.equals("ITEM")) {
//            if(Float.parseFloat(cache.get(trimmedItem + "_price")) != price) {
//                return false;
//            } else {
//                int storedQty = Integer.parseInt((String)this.DB.get(trimmedItem + "_qty"));
//                if(qty >= 1 && storedQty >= qty) {
//                    storedQty -= qty;
//                    cache.put(trimmedItem + "_qty", "" + storedQty);
//                    System.out.println("purchase: " + item +" qty:" + qty);
//                    return true;
//                } else {
//                    return false;
//                }
//            }
//        } else {
//            return false;
//        }
//    }


    public synchronized void shutDown() throws RemoteException {
        UnicastRemoteObject.unexportObject(this, true);
    }

    static class LRUCache<K, V> extends ConcurrentHashMap<K, V> {
        int size;
        public LRUCache(int size) {
            // true for access order
            super(size, 0.75f);
            this.size = size;
        }
    }

}

