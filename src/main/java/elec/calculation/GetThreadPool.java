package elec.calculation;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class GetThreadPool {
	public ExecutorService executorService;
	public GetThreadPool(int threadPoolNumber){
		this.executorService=Executors.newFixedThreadPool(threadPoolNumber);
	}

	public ExecutorService getExecutorService() {
		return executorService;
	}

	public void shutDownExecutrorService() {
		executorService.shutdown();
	}
	
	public Future<Map<Long, Float>>  submit(Callable<Map<Long, Float>> task){
		return this.getExecutorService().submit(task);
	}
}
