
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

//    private static ConcurrentLinkedQueue <Cloud.FrontEndOps.Request> centralizedQueue;
    private static ConcurrentLinkedQueue <WrapperReq> centralizedQueue;
    private static long lastScaleOutAppTime;
    private static long lastScaleOutForTime;
    private static final long SCALE_OUT_APP_THRESHOLD = 60000;
    private static final long SCALE_OUT_FOR_THRESHOLD = 60000;
    private static final long MAX_FORWARDER_NUM = 1;
    private static final long MAX_APP_NUM = 12;

    // especially for cache
    private static ConcurrentHashMap<String, String> cache;
    private static Cloud.DatabaseOps cacheIntf = null;
    private static Cloud.DatabaseOps DB = null;

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
            } else if (interval < 300) {
                startNum = 5;
                startForNum = 0;
            } else if (interval < 650) {
                startNum = 4;
                startForNum = 0;
            } else if (interval < 800) {
                startNum = 1;
                startForNum = 0;
            } else {
                startNum = 1;
                startForNum = 0;
            }

            for (int i = 0; i < startNum; ++i) {
                SL.startVM();
            }
            lastScaleOutAppTime = 0;
            lastScaleOutForTime = 0;
//            lastScaleOutAppTime = System.currentTimeMillis();

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
                    int queLen = SL.getQueueLength();
//                    System.out.println("queLen" + queLen);

//                    if (queLen > appServerList.size() * 1.5){
                    if (queLen > appServerList.size() * 1.5){
//                        System.out.println("queLen" + queLen);
                        scaleOutFor(1);
                        int number = (int)(queLen/appServerList.size());
//                        System.out.println("try scale out number:" + number);
                        scaleOutApp(number);
                    }

                    if (centralizedQueue.size() > appServerList.size()) {
                        while (centralizedQueue.size() > appServerList.size() * 1.5) {
                            SL.drop(centralizedQueue.poll().request);
                        }
                    } else {

                        while ((r = SL.getNextRequest()) == null) { }
                        centralizedQueue.add(new WrapperReq(r, System.currentTimeMillis()));
                    }
                }
                catch (Exception e){
//                    e.printStackTrace();
                    continue;
                }
            }
        }

        //non-master vm
        else{
            while (true) {
                try {
                    masterIntf = (ServerIntf) LocateRegistry.getRegistry(selfIP, basePort).lookup("//localhost/no1");
                    break;
                } catch (Exception e) {
                    continue;
                }
            }


            // getrole
            Content reply = null;
            try {
                reply = masterIntf.getRole(vmId);
            } catch (Exception e){
//                e.printStackTrace();
                return;
            }

            // do work
            // forwarder
            if(reply.role == FORWARDER) {
                System.out.println("==========Forwarder");
                SL.register_frontend();
                Cloud.FrontEndOps.Request r = null;
                while (true) {
//                    while (SL.getQueueLength() > 6){
//                        SL.dropHead();
//                    }
                    while ((r = SL.getNextRequest()) == null){}
                    masterIntf.addToCentralizedQueue(new WrapperReq(r, System.currentTimeMillis()));
                }
            }
            // processor
            else{
                System.out.println("==========App, interval:");
                cacheIntf = (Cloud.DatabaseOps) masterIntf;
                while(true) {
                    try {
                        WrapperReq r = masterIntf.getFromCentralizedQueue();
                        if (r.isTimeout()){
                            SL.drop(r.request);
//                            System.out.println("drop");
                        } else {
//                            System.out.println("process");
                            SL.processRequest(r.request, cacheIntf);
                        }
                    } catch (Exception e){
//                        e.printStackTrace();
                        continue;
                    }

                }
            }
        }
    }

    public static void scaleOutApp(int number){
        number = Math.max(number, 7);
        number = Math.min(number, 10);
        if (appServerList.size() < MAX_APP_NUM && System.currentTimeMillis() - lastScaleOutAppTime >= SCALE_OUT_APP_THRESHOLD) {
            System.out.println("scaleOutApp:" + number);
            for (int i = 0; i < Math.min(number, MAX_APP_NUM - appServerList.size()); ++i) {
                SL.startVM();
            }
            lastScaleOutAppTime = System.currentTimeMillis();
        }else {
//            System.out.println("scaleOutRefused");
        }
    }


    public static void scaleOutFor(int number){
        if (forServerList.size() < MAX_FORWARDER_NUM && System.currentTimeMillis() - lastScaleOutForTime >= SCALE_OUT_FOR_THRESHOLD) {
            System.out.println("scale out for");
            forServerList.add(SL.startVM());
            lastScaleOutForTime = System.currentTimeMillis();
        }else {
//            System.out.println("scaleOutRefused");
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

    public WrapperReq getFromCentralizedQueue() throws RemoteException{
        WrapperReq r = null;
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


    public void addToCentralizedQueue (WrapperReq r) throws RemoteException{
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


    public synchronized boolean transaction(String item, float price, int qty) throws RemoteException {
        boolean ret = DB.transaction(item, price, qty);
        if (ret) {
//             update: add change to cache
            String trimmedItem = item.trim();
            String qtyStr = trimmedItem + "_qty";
            cache.put(qtyStr, String.valueOf(Integer.parseInt(cache.get(qtyStr))-qty));
        }
//        System.out.println("purchase: " + item +" qty:" + qty + "ret:" + ret);
        return ret;
    }


    public synchronized void shutDown() throws RemoteException {
        UnicastRemoteObject.unexportObject(this, true);
    }

}
