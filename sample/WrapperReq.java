import java.io.Serializable;

/**
 * Created by Lu on 4/1/16.
 */
public class WrapperReq implements Serializable {
        long getTime;
        Cloud.FrontEndOps.Request request;
        boolean isPurchase;
        WrapperReq(Cloud.FrontEndOps.Request r,long time){
            this.request = r;
            this.getTime = time;
            this.isPurchase = r.isPurchase;
        }
        boolean isTimeout(){
            if (isPurchase){
                long period = System.currentTimeMillis() - getTime;
                System.out.println("Purchase:" + period);
                return (period > 1200);
            }
            long period = System.currentTimeMillis() - getTime;
            System.out.println("NotPurchase:" + period);
            return (period) > 530;
        }

    }

