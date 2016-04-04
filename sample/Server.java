import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.*;
import java.util.*;
<<<<<<< HEAD

public class Server extends UnicastRemoteObject implements ServerIntf, Cloud.DatabaseOps {

    private static final int MASTER = 1;
    private static final Boolean FORWARDER = true;
    private static final Boolean PROCESSOR = false;
    private static ServerIntf masterIntf;
    private static List<Integer> forServerList;
    private static List<Integer> appServerList;
    private static ServerLib SL;
    private static long interval = 1000;
    private static int startNum = 1;
    private static int startForNum = 0;
    private static int vmId;
    private static int basePort;
    private static String selfIP;

    private static ConcurrentLinkedQueue <Cloud.FrontEndOps.Request> centralizedQueue;
    private static long lastScaleOutAppTime;
    private static long lastScaleOutForTime;
    private static long lastScaleInTime;
    private static final long SCALE_OUT_APP_THRESHOLD = 60000;
    private static final long SCALE_OUT_FOR_THRESHOLD = 60000;
    private static final long SCALE_In_THRESHOLD = 60000;
    private static final long MAX_FORWARDER_NUM = 1;
    private static final long MAX_APP_NUM = 12;
=======
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
    private static ConcurrentLinkedDeque<Cloud.FrontEndOps.Request> localReqQueue;
	private static ServerIntf masterIntf;
	private static int selfRPCPort;
    private static int basePort;
	private static boolean selfRole;
	private static String selfIP;
	private static ServerLib SL;
	private static int curRound = 0;
	private static long appLastScaleoutTime;
    private static long appLastScaleinTime;
    private static long forLastScaleoutTime;
    private static long interval = 1000;
    private static final long APP_ADD_COOL_DOWN_INTERVAL = 30000;
    private static final long APP_REMOVE_COOL_DOWN_INTERVAL = 20000;
    private static final long FOR_ADD_COOL_DOWN_INTERVAL = 20000;
    private static final long MAX_FORWARDER_NUM = 1;
    private static final long MAX_APP_NUM = 10;
    private static int startNum = 1;
    private static int startForNum = 0;
    private static int newAppNum = 0;
    private static int goneAppNum = 0;
    private static final int REDUCE_INTERVAL_HIT = 3;
    private static final int ADD_INTERVAL_HIT = 2;
>>>>>>> 8ad16dfd4f8f6622598778f39044bd8472376017

    // especially for cache
    private static ConcurrentHashMap<String, String> cache;
    private static Cloud.DatabaseOps cacheIntf = null;
    private static Cloud.DatabaseOps DB = null;

    private static boolean kill = false;
    private static boolean unregister = false;

    public Server() throws RemoteException {

    }


    public static void main (String args[] ) throws Exception {

        if (args.length != 3) throw new Exception("Need 3 args: <cloud_ip> <cloud_port>");
        selfIP = args[0];
        basePort = Integer.parseInt(args[1]);
        vmId = Integer.parseInt(args[2]);
        System.out.println("---vmID----" + vmId);

        SL = new ServerLib(selfIP, basePort);
        LocateRegistry.getRegistry(selfIP, basePort).bind("//localhost/no" + String.valueOf(vmId), new Server());

<<<<<<< HEAD
=======
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
                } else if (interval < 200) {
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
>>>>>>> 8ad16dfd4f8f6622598778f39044bd8472376017

        if(vmId == MASTER) {
            SL.startVM();
            forServerList = Collections.synchronizedList(new ArrayList<>());
            appServerList = Collections.synchronizedList(new ArrayList<>());
            DB = SL.getDB();
            cache = new ConcurrentHashMap<>(512);
            centralizedQueue = new ConcurrentLinkedQueue<>();

            SL.register_frontend();
            while(SL.getQueueLength() == 0 );
            long time1 = System.currentTimeMillis();
            SL.dropHead();
            while(SL.getQueueLength() == 0 );
            long time2 = System.currentTimeMillis();
            interval = time2 - time1;
            System.out.println("time2-time1:" + interval);

            if (interval < 130) {
                startNum = 7;
                startForNum = 1;
            } else if (interval < 200) {
                startNum = 6;
                startForNum = 1;
            } else if (interval < 650) {
                startNum = 3;
                startForNum = 0;
            } else {
                startNum = 1;
                startForNum = 0;
            }

            for (int i = 0; i < startNum; ++i) {
                SL.startVM();
            }

<<<<<<< HEAD
            if (interval < 300) {
                lastScaleInTime = 0 ;
                lastScaleOutAppTime = System.currentTimeMillis();
                lastScaleOutForTime = System.currentTimeMillis();
            }else {
                lastScaleInTime = System.currentTimeMillis();
                lastScaleOutAppTime = 0;
                lastScaleOutForTime = 0;
            }
=======
                for (int i = 0; i < Math.min(3, startNum) ; ++i) {
                    futureAppServerList.put(SL.startVM() + basePort, true);
                }
>>>>>>> 8ad16dfd4f8f6622598778f39044bd8472376017

            for (int i = 0; i < startForNum; ++i) {
                forServerList.add(SL.startVM());
            }

<<<<<<< HEAD
            while( appServerList.size() == 0){
                SL.dropHead();
                Thread.sleep(50);
            }
=======
                for (int i = 3; i < startNum - 1; ++i) {
                    futureAppServerList.put(SL.startVM() + basePort, true);
                }

//                appLastScaleoutTime = System.currentTimeMillis();
                appLastScaleoutTime = 0;
                appLastScaleinTime = System.currentTimeMillis();
>>>>>>> 8ad16dfd4f8f6622598778f39044bd8472376017

            System.out.println("interval:" + interval + " start:" + startNum + " startFor:" + startForNum);
            Cloud.FrontEndOps.Request r = null;
            while (true) {
                try {
                    // consider scaleout
                    int queLen = SL.getQueueLength();
                    if (queLen > appServerList.size() * 1.5){
                        scaleOutFor(1);
                        int number = (int)(queLen/appServerList.size()*4);
                        scaleOutApp(number);
                    }

                    // if queue is too long, drop head
                    if (centralizedQueue.size() > appServerList.size()) {
                        while (centralizedQueue.size() > appServerList.size() * 1.5) {
                            SL.drop(centralizedQueue.poll());
                        }
                    } else {
                        // consider scalein
                        long lastTimeGetReq = System.currentTimeMillis();
                        while ((r = SL.getNextRequest()) == null) {
                        }
                        long period = System.currentTimeMillis() - lastTimeGetReq;

                        if (period > interval * 3){
                            int scaleInAppNumber = (int) (period - interval)/40;
                            int scaleInForNumber = scaleInAppNumber > 5 ? 1 : 0;
                            if (scaleIn(scaleInAppNumber, scaleInForNumber)) {
                                interval = period;
                            }
                        }
                        centralizedQueue.add(r);
                    }
                }
                catch (Exception e){
                    continue;
                }
            }
        }

        //non-master vm
        else{
            // lookup master (and cache)
            while (true) {
                try {
                    masterIntf = (ServerIntf) LocateRegistry.getRegistry(selfIP, basePort).lookup("//localhost/no1");
                    break;
                } catch (Exception e) {
                    continue;
                }
<<<<<<< HEAD
            }
=======
                int dropBCCongestion  = 0;
                while (true) {
                    if (vmId == MASTER) {
                        if (newAppNum >= REDUCE_INTERVAL_HIT){
                            if (interval > 200){
                                broadcastInterval(200);
                            }
                            newAppNum = 0;
                            goneAppNum = 0;
                        } else if (goneAppNum >= ADD_INTERVAL_HIT){
                            broadcastInterval(1000);
                            newAppNum = 0;
                            goneAppNum = 0;
                        }

                        while (SL.getQueueLength() > 5) {
                            SL.dropHead();
                            dropBCCongestion++;
                            System.out.println("drop b.c. congestion:" + dropBCCongestion);
                        }
                        Cloud.FrontEndOps.Request r = SL.getNextRequest();
                        if (r == null) {
                            continue;
                        }
                        forwardReq2(r);
>>>>>>> 8ad16dfd4f8f6622598778f39044bd8472376017

            // get role
            Content reply = null;
            try {
                reply = masterIntf.getRole(vmId);
            } catch (Exception e){
                return;
            }

<<<<<<< HEAD
            // do work
            // forwarder
            if(reply.role == FORWARDER) {
                System.out.println("==========Forwarder");
                SL.register_frontend();
                Cloud.FrontEndOps.Request r = null;
                while (true) {
                    while ((r = SL.getNextRequest()) == null){}
                    masterIntf.addToCentralizedQueue(r);
                    // if have to kill self
                    if (kill){
                        if (unregister == false) {
                            SL.unregister_frontend();
                            unregister = true;
=======
                    } else {
                        while (SL.getQueueLength() > 5 ) {
                            SL.dropHead();
                            dropBCCongestion++;
                            System.out.println("drop b.c. congestion:" + dropBCCongestion);
>>>>>>> 8ad16dfd4f8f6622598778f39044bd8472376017
                        }
                        if (SL.getQueueLength() == 0){
                            masterIntf.killMe(vmId, FORWARDER);
                            Thread.sleep(5000);
                        }
                    }
                }
            }
            // processor
            else{
                System.out.println("==========App, interval:");
                cacheIntf = (Cloud.DatabaseOps) masterIntf;
<<<<<<< HEAD
                while(true) {
                    try {
                        Cloud.FrontEndOps.Request r = masterIntf.getFromCentralizedQueue();
                        SL.processRequest(r, cacheIntf);
                        if (kill){
                            masterIntf.killMe(vmId, PROCESSOR);
                            Thread.sleep(5000);
=======
                long idleBeginTime = 0;
                appLastScaleinTime = System.currentTimeMillis();
                while (true) {
                    if (idleBeginTime == Long.MAX_VALUE){
                        idleBeginTime = System.currentTimeMillis();
                    }
                    if (localReqQueue.size() > 1) {
                        idleBeginTime = Long.MAX_VALUE;
                        SL.processRequest(localReqQueue.poll(), cacheIntf);
                        int firstFetchNum = localReqQueue.size();
                        System.out.println("firstFetchNum" + firstFetchNum);

                        while (interval <= 800 && localReqQueue.size() > 3) {
                            System.out.println("localReqQueue.size()" + localReqQueue.size());
                            Cloud.FrontEndOps.Request  r = localReqQueue.poll();
                            SL.drop(r);
                        }

                        while (interval <= 400 && localReqQueue.size() > 1) {
                            System.out.println("localReqQueue.size()" + localReqQueue.size());
                            Cloud.FrontEndOps.Request  r = localReqQueue.poll();
                            SL.drop(r);
                        }

                        if (firstFetchNum > 1 && System.currentTimeMillis() - lastTime > APP_ADD_COOL_DOWN_INTERVAL) {
                            lastTime = System.currentTimeMillis();
                            int scaleUpNum = (int) (((double)(firstFetchNum) * 2) );
                            System.out.println("asking for scale up app:" + scaleUpNum);
                            if (scaleUpNum != 0) {
                                masterIntf.scaleOutApp(scaleUpNum);
                            }
>>>>>>> 8ad16dfd4f8f6622598778f39044bd8472376017
                        }
                    } catch (Exception e){
//                        e.printStackTrace();
                        continue;
                    }
<<<<<<< HEAD
=======
                    if ((curReq = localReqQueue.poll()) != null) {
                        idleBeginTime = Long.MAX_VALUE;
                        SL.processRequest(curReq, cacheIntf);
                    }
                    if (System.currentTimeMillis() - idleBeginTime > 800 &&
                            System.currentTimeMillis() - appLastScaleinTime >= APP_REMOVE_COOL_DOWN_INTERVAL){
                        System.out.println("idle hit");
                        try {
                            masterIntf.scaleInApp(7);
                        } catch (java.rmi.ConnectException | java.rmi.ConnectIOException | java.rmi.UnmarshalException e1){
                            ;
                        }

                        idleBeginTime = System.currentTimeMillis();
                        appLastScaleinTime = System.currentTimeMillis();
                    }
>>>>>>> 8ad16dfd4f8f6622598778f39044bd8472376017

                }
            }
        }
<<<<<<< HEAD
    }

    public void killMe(Integer vmId, boolean type) throws RemoteException{
        if (type == FORWARDER){
            System.out.println("kill forwarder:" + vmId);
            forServerList.remove(vmId);
            SL.endVM(vmId);
        } else {
            System.out.println("kill processor:" + vmId);
            appServerList.remove(vmId);
            SL.endVM(vmId);
=======
	}



    public synchronized void scaleInApp(int num) throws Exception{
        System.out.println("Receiving scale in app request:" + num);

        if (System.currentTimeMillis() - appLastScaleinTime > APP_REMOVE_COOL_DOWN_INTERVAL) {
            System.out.println("Check whther to scale in");
            int curNum = appServerList.size();
            num = Math.min(num, curNum - 1);
            goneAppNum += num;
            for (int i = 0; i < num; ++i) {
                Integer appToRemove = 0;
                try {
                    appToRemove = appServerList.remove(i);
                }catch (java.lang.IndexOutOfBoundsException e){
                    break;
                }
                synchronized (forServerList) {
                    for (int j = 0; i < forServerList.size(); ++i) {
                        int rpcPort = forServerList.get(i);
                        Registry reg = LocateRegistry.getRegistry(selfIP, basePort);
                        ServerIntf curForIntf = (ServerIntf) reg.lookup("//localhost/no" + rpcPort);
                        curForIntf.goodbyeApp(appToRemove);
                    }
                }
                Thread.sleep(1000);
                SL.endVM(appToRemove - basePort);
            }
            System.out.println("App scale in OK");
            appLastScaleinTime = System.currentTimeMillis();
        }else {
            System.out.println("App scale in Rejected");
        }

    }

	public void scaleOutApp(int num) throws Exception{
        System.out.println("Receiving scale out app request:" + num);
        int curNum = appServerList.size() + futureAppServerList.size();
        num = Math.min(num, 6);
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
>>>>>>> 8ad16dfd4f8f6622598778f39044bd8472376017
        }

    }
    public void killYourself() throws RemoteException{
        kill = true;
    }

    public static boolean scaleIn(int appNumber, int forNumber) throws Exception{
        appNumber = Math.min(appServerList.size()-1, appNumber);
        forNumber = Math.min(forServerList.size(), forNumber);
        if (System.currentTimeMillis() - lastScaleInTime >= SCALE_In_THRESHOLD) {
            // scale in app
            for (int i = 0; i < appNumber; ++i){
                int vmId = appServerList.remove(appServerList.size()-1);
                System.out.println("endApp:"+vmId);
                ServerIntf curServer = (ServerIntf) LocateRegistry.getRegistry(selfIP, basePort).lookup("//localhost/no"+String.valueOf(vmId));
                curServer.killYourself();
            }

            for (int i = 0; i < forNumber; ++i){
                int vmId = forServerList.remove(0);
                System.out.println("endFor:"+vmId);
                ServerIntf curServer = (ServerIntf) LocateRegistry.getRegistry(selfIP, basePort).lookup("//localhost/no"+String.valueOf(vmId));
                curServer.killYourself();
            }
            lastScaleInTime = System.currentTimeMillis();
            return true;
        }
        return false;
    }

<<<<<<< HEAD
    public static void scaleOutApp(int number){
        int originalNum = number;
        number = Math.min(number, 7);
        if (appServerList.size() < MAX_APP_NUM && System.currentTimeMillis() - lastScaleOutAppTime >= SCALE_OUT_APP_THRESHOLD) {
            System.out.println("original number:" + originalNum);
            System.out.println("scaleOutApp:" + number);
            for (int i = 0; i < Math.min(number, MAX_APP_NUM - appServerList.size()); ++i) {
                SL.startVM();
            }
            lastScaleOutAppTime = System.currentTimeMillis();
        }
=======
    public void goodbyeApp(Integer rpcPort) throws RemoteException{
        System.out.println("goodbye, " + rpcPort);
        appServerList.remove(rpcPort);
    }

    public void welcomeNewApp(Integer rpcPort) throws RemoteException{
        appServerList.add(rpcPort);
>>>>>>> 8ad16dfd4f8f6622598778f39044bd8472376017
    }


    public static void scaleOutFor(int number){
        if (forServerList.size() < MAX_FORWARDER_NUM && System.currentTimeMillis() - lastScaleOutForTime >= SCALE_OUT_FOR_THRESHOLD) {
            System.out.println("scale out for");
            forServerList.add(SL.startVM());
            lastScaleOutForTime = System.currentTimeMillis();
        }
    }

    public Content getRole (Integer vmID) throws RemoteException{

        if( !forServerList.contains(vmID) ) {
            System.out.println("vmID :" + vmID + " is app`");
            appServerList.add(vmID);
            return new Content(PROCESSOR);
        }
<<<<<<< HEAD
        else {
            System.out.println("vmID :" + vmID + " is for");
            return new Content(FORWARDER);
        }
=======
        if (curRound > appServerList.size()-1){
            curRound = (curRound + 1) % appServerList.size();
        }
        while (true){
            int curPort = appServerList.get(curRound);
            ServerIntf curAppIntf = null;
            try {
                Registry reg = LocateRegistry.getRegistry(selfIP, basePort);
                curAppIntf = (ServerIntf) reg.lookup("//localhost/no"+curPort);
            }catch (Exception e){
                e.printStackTrace();
                continue;
            }
            try {
                curAppIntf.addToLocalQue(r);
                break;
            } catch (Exception e){
//                e.printStackTrace();
                curRound = (curRound + 1) % appServerList.size();
            }
        }
        curRound = (curRound + 1) % appServerList.size();
    }

    public void addToLocalQue(Cloud.FrontEndOps.Request r) throws Exception{
        localReqQueue.add(r);
>>>>>>> 8ad16dfd4f8f6622598778f39044bd8472376017
    }

    public Cloud.FrontEndOps.Request getFromCentralizedQueue() throws RemoteException{
        Cloud.FrontEndOps.Request r = null;
        while (true){
            r = centralizedQueue.poll();
            if (r == null){
                continue;
            }else {
                break;
            }
        }
        return r;
    }


    public void addToCentralizedQueue (Cloud.FrontEndOps.Request r) throws RemoteException{
        centralizedQueue.add(r);
    }


    public String get(String key) throws RemoteException{

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
        //write through
        System.out.println("CallSet:" + password);
        boolean ret = DB.set(key, value, password);
        // if success, insert in cache
        if (ret){
            cache.put(key, value);
            return true;
        }
        return false;
    }


    public synchronized boolean transaction(String item, float price, int qty) throws RemoteException {
<<<<<<< HEAD
        boolean ret = DB.transaction(item, price, qty);
        if (ret) {
//             update: add change to cache
            String trimmedItem = item.trim();
            String qtyStr = trimmedItem + "_qty";
            cache.put(qtyStr, String.valueOf(Integer.parseInt(cache.get(qtyStr))-qty));
=======
        String trimmedItem = item.trim();
        String value = cache.get(trimmedItem);
        if(value != null && value.equals("ITEM")) {
            if(Float.parseFloat(cache.get(trimmedItem + "_price")) != price) {
                return false;
            } else {
                int storedQty = Integer.parseInt((String)this.DB.get(trimmedItem + "_qty"));
                if(qty >= 1 && storedQty >= qty) {
                    storedQty -= qty;
                    cache.put(trimmedItem + "_qty", "" + storedQty);
                    DB.set(trimmedItem+"_qty", storedQty+"", "sqwe");
//                    System.out.println("purchase: " + item +" qty:" + qty);
                    return true;
                } else {
                    return false;
                }
            }
        } else {
            return false;
>>>>>>> 8ad16dfd4f8f6622598778f39044bd8472376017
        }
        return ret;
    }


    public synchronized void shutDown() throws RemoteException {
        UnicastRemoteObject.unexportObject(this, true);
    }

}
