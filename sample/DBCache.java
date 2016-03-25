import Cloud.DatabaseOps;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.LinkedHashMap;

/**
 * Created by Lu on 3/25/16.
 */



public class DBCache extends  UnicastRemoteObject implements Cloud.DatabaseOps {
    private final HashMap<String, String> DB;
    private final String authString;
    private int delayTime = 0;
    //todo: change to static?
    private LRUCache<String, String> cache = new LRUCache<String, String>(64);
    private static ServerLib SL;

    class LRUCache<K, V> extends LinkedHashMap<K, V>{
        int size;
        int port;
        String ip;
        public LRUCache(int size, String ip, int port){
            // true for access order
            super(size, 0.75f, true);
            this.size = size;
            SL = new ServerLib(ip, port);
        }
    }

    public DBCache(String var1) throws RemoteException {
        super(0);
        this.DB = new HashMap();
        this.authString = var1;
    }

    public DBCache(DBCache var1, String var2) throws RemoteException {
        super(0);
        this.DB = new HashMap(var1.DB);
        this.authString = var2;
    }

    public DBCache(String var1, String var2) throws RemoteException {
        super(0);
        this.DB = new HashMap();
        this.authString = var2;

        try {
            BufferedReader var3 = new BufferedReader(new FileReader(new File(var1)));
            Throwable var4 = null;

            try {
                String var5 = null;

                while(true) {
                    String[] var6;
                    String var7;
                    String var8;
                    do {
                        do {
                            if((var5 = var3.readLine()) == null) {
                                var3.close();
                                return;
                            }

                            var6 = var5.split("#", 2)[0].split(":");
                        } while(var6.length == 1);

                        var7 = var6[0].trim();
                        var8 = var6[1].trim();
                    } while(var6.length != 2 && (!var8.equals("ITEM") || var6.length != 4));

                    this.DB.put(var7, var8);
                    if(var8.equals("ITEM")) {
                        float var9 = 10.0F;
                        int var10 = 100;
                        if(var6.length > 2) {
                            var9 = Float.parseFloat(var6[2].trim());
                        }

                        if(var6.length > 3) {
                            var10 = Integer.parseInt(var6[3].trim());
                        }

                        this.DB.put(var7 + "_price", "" + var9);
                        this.DB.put(var7 + "_qty", "" + var10);
                    }
                }
            } catch (Throwable var19) {
                var4 = var19;
                throw var19;
            } finally {
                if(var3 != null) {
                    if(var4 != null) {
                        try {
                            var3.close();
                        } catch (Throwable var18) {
                            var4.addSuppressed(var18);
                        }
                    } else {
                        var3.close();
                    }
                }

            }
        } catch (Exception var21) {
            var21.printStackTrace();
        }
    }

    public synchronized void shutDown() throws RemoteException {
        UnicastRemoteObject.unexportObject(this, true);
    }

    public synchronized void setDelay(int var1, String var2) {
        if(var2.equals(this.authString)) {
            this.delayTime = var1;
        }
    }

    private synchronized void delay() {
        if(this.delayTime > 0) {
            Cloud.work(this.delayTime);
        }

    }

    public synchronized String get(String var1) throws RemoteException {
        this.delay();
        return (String)this.DB.get(var1.trim());
    }

    public synchronized boolean set(String var1, String var2, String var3) throws RemoteException {
        if(!var3.equals(this.authString)) {
            return false;
        } else {
            this.delay();
            this.DB.put(var1.trim(), var2.trim());
            return true;
        }
    }

    public synchronized boolean transaction(String var1, float var2, int var3) throws RemoteException {
        String var4 = var1.trim();
        String var5 = (String)this.DB.get(var4);
        this.delay();
        if(var5 != null && var5.equals("ITEM")) {
            this.delay();
            if(Float.parseFloat((String)this.DB.get(var4 + "_price")) != var2) {
                return false;
            } else {
                this.delay();
                int var6 = Integer.parseInt((String)this.DB.get(var4 + "_qty"));
                if(var3 >= 1 && var6 >= var3) {
                    this.delay();
                    var6 -= var3;
                    this.DB.put(var4 + "_qty", "" + var6);
                    return true;
                } else {
                    return false;
                }
            }
        } else {
            return false;
        }
    }
}
