/* Sample code for basic Server */

public class Server {
	private static final int VM_FOR_LAUNCHING = 1;
	public static void main ( String args[] ) throws Exception {
		if (args.length != 3) throw new Exception("Need 3 args: <cloud_ip> <cloud_port>");
		ServerLib SL = new ServerLib( args[0], Integer.parseInt(args[1]) );

		System.out.println("---0----" + args[0]);
		System.out.println("---1----" + args[1]);
		System.out.println("---2----" + args[2]);

		int vmId = Integer.parseInt(args[2]);

		// register with load balancer so requests are sent to this server
		SL.register_frontend();

		// vm No.1 is responsible for instantiating enough vms
		if (vmId == VM_FOR_LAUNCHING){
			// get # of current needed vms
			float curTime = SL.getTime();
			int num = getVMNumber(curTime);
			for (int i = 0; i < num-1; ++i){
				SL.startVM();
			}
		}

		// main loop
		while (true) {
			Cloud.FrontEndOps.Request r = SL.getNextRequest();
			SL.processRequest( r );
		}
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

