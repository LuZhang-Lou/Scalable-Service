import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.*;
import java.util.*;

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

            if (interval < 300) {
                lastScaleInTime = 0 ;
                lastScaleOutAppTime = System.currentTimeMillis();
                lastScaleOutForTime = System.currentTimeMillis();
            }else {
                lastScaleInTime = System.currentTimeMillis();
                lastScaleOutAppTime = 0;
                lastScaleOutForTime = 0;
            }

            for (int i = 0; i < startForNum; ++i) {
                forServerList.add(SL.startVM());
            }

            while( appServerList.size() == 0){
                SL.dropHead();
                Thread.sleep(50);
            }

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
            }

            // get role
            Content reply = null;
            try {
                reply = masterIntf.getRole(vmId);
            } catch (Exception e){
                return;
            }

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
                while(true) {
                    try {
                        Cloud.FrontEndOps.Request r = masterIntf.getFromCentralizedQueue();
                        SL.processRequest(r, cacheIntf);
                        if (kill){
                            masterIntf.killMe(vmId, PROCESSOR);
                            Thread.sleep(5000);
                        }
                    } catch (Exception e){
//                        e.printStackTrace();
                        continue;
                    }

                }
            }
        }
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
        else {
            System.out.println("vmID :" + vmID + " is for");
            return new Content(FORWARDER);
        }
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
        boolean ret = DB.transaction(item, price, qty);
        if (ret) {
//             update: add change to cache
            String trimmedItem = item.trim();
            String qtyStr = trimmedItem + "_qty";
            cache.put(qtyStr, String.valueOf(Integer.parseInt(cache.get(qtyStr))-qty));
        }
        return ret;
    }


    public synchronized void shutDown() throws RemoteException {
        UnicastRemoteObject.unexportObject(this, true);
    }

}
