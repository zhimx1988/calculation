package elec.Calculation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import elec.calculation.RealTimePrice;

@Controller
public class CalCostOnDateNode {
	@RequestMapping(value = "/test", method = RequestMethod.GET)
	@ResponseBody
	public String test(@RequestParam long startId,
			@RequestParam long endId){
		return startId+":"+endId;
	}
	
	
	@RequestMapping(value = "/calculationUsage", method = RequestMethod.GET)
	@ResponseBody
	public String calculationCost(@RequestParam long startId,
			@RequestParam long endId, @RequestParam long startTime,
			@RequestParam long endTime) {
		Timestamp starTimestamp=new Timestamp(startTime);
		Timestamp endTimestamp=new Timestamp(endTime);
		String string="";
		try {
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		try {
			ArrayList<RealTimePrice> priceList;
				priceList = getPriceList(getLastUpdatePrice(starTimestamp),
						endTimestamp);
			string = calCost(starTimestamp, endTimestamp, startId, endId, priceList);
			Date date=new Date();
			System.out.println(date.getTime());
			return string;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return "false";
		} catch (ExecutionException e) {
			e.printStackTrace();
			return "false";
		}catch (SQLException e) {
			e.printStackTrace();
			return "false";
		}
		
	}

	private String calCost(Timestamp startTime, Timestamp endTime,
			long startId, long endId, ArrayList<RealTimePrice> priceList)
			throws InterruptedException, ExecutionException {
		List<Future<String>> list = new ArrayList<Future<String>>();
		long length = (endId - startId) / 10, min = startId, max = startId
				+ length;
		int i = 1;
		GetThreadPool queryThreadPoolWithBigGroup = new GetThreadPool(2);
		for (; i <= 10; i++) {
			if (i != 10) {
				list.add(queryThreadPoolWithBigGroup.getExecutorService()
						.submit(new CalculationCostWithSmallGroup(min, max,
								startTime, endTime, priceList)));
				min = max;
				max = min + length;
			} else {
				list.add(queryThreadPoolWithBigGroup.getExecutorService()
						.submit(new CalculationCostWithSmallGroup(min,
								endId + 1, startTime, endTime, priceList)));
			}
		}
		for (Future<String> future : list) {
			future.get();
		}
		System.out.println("finished");
		return "finished";
	}

	class CalculationCostWithSmallGroup implements Callable<String> {
		// get last sum for each device
		String LastSql = "select DEVID, MAX(Sum) AS LastSum from PH_METERLOGDATA where DEVID >=? AND DEVID < ? AND TIME > ? AND TIME < ? Group by DEVID";
		// insert the resut
		String deviceSql = "UPSERT INTO PH_COST( DEVID,TIME,USAGE,COST ) VALUES(?,?,?,?)";
		// get meter data
		String getMeterLogSql = "select DEVID,TIME,SUM from PH_METERLOGDATA where DEVID>=? AND DEVID<? AND TIME >= ? AND TIME < ? ORDER BY TIME ASC NULLS LAST";

		Timestamp startTimestamp;
		Timestamp endTimestamp;
		long startId;
		long endId;
		ArrayList<RealTimePrice> priceList = new ArrayList<RealTimePrice>();
		Map<Long, Float> lastUpdateSum = new HashMap<Long, Float>();

		public CalculationCostWithSmallGroup(long startId, long endId,
				Timestamp startTimestamp, Timestamp endTimestamp,
				ArrayList<RealTimePrice> priceList) {
			this.startTimestamp = startTimestamp;
			this.endTimestamp = endTimestamp;
			this.startId = startId;
			this.endId = endId;
			this.priceList = priceList;
		}

		public String call() throws SQLException {
			Connection conn = null;
			ResultSet rSet = null;
			Timestamp lastDate = getLastDay(new Date(startTimestamp.getTime()));
			try {
				conn = DriverManager.getConnection(Utils.CONNECT_STRING);
				PreparedStatement LastSumStatement = conn
						.prepareStatement(LastSql);
				PreparedStatement MeterLogStatement = conn
						.prepareStatement(getMeterLogSql);
				PreparedStatement DevInsertStatement = conn
						.prepareStatement(deviceSql);
				LastSumStatement.setLong(1, startId);
				LastSumStatement.setLong(2, endId);
				LastSumStatement.setTimestamp(3, lastDate);
				LastSumStatement.setTimestamp(4, startTimestamp);
				rSet = LastSumStatement.executeQuery();
				while (rSet != null && rSet.next()) {
					Long devId = rSet.getLong(1);
					float LastSum = rSet.getFloat(2);
					lastUpdateSum.put(devId, LastSum);
				}
				rSet.close();
				MeterLogStatement.setLong(1, startId);
				MeterLogStatement.setLong(2, endId);
				MeterLogStatement.setTimestamp(3, startTimestamp);
				MeterLogStatement.setTimestamp(4, endTimestamp);
				rSet = MeterLogStatement.executeQuery();
				int i=1;
				while (rSet != null && rSet.next()) {
					Long devId = rSet.getLong(1);
					Timestamp time = rSet.getTimestamp(2);
					float Sum = rSet.getFloat(3);
					Float lastSum = lastUpdateSum.get(devId);
					float usage;
					if (lastSum == null)
						usage = Sum;
					else
						usage = Sum - lastSum;
					lastUpdateSum.put(devId, Sum);
					RealTimePrice lastHappenPrice = RealTimePrice.lower_bound(
							priceList, time);

					float cost = usage * lastHappenPrice.price;

					DevInsertStatement.setLong(1, devId);
					DevInsertStatement.setTimestamp(2, time);
					DevInsertStatement.setFloat(3, usage);
					DevInsertStatement.setFloat(4, cost);
					DevInsertStatement.execute();
					/*System.out.println("devid:" + devId + "----time:" + time
							+ "----usage:" + usage + "----cost:" + cost);*/
					if(i++%5000==0)
						conn.commit();
				}
				conn.commit();

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (!conn.isClosed()) {
					conn.close();
				}
				if (!rSet.isClosed()) {
					rSet.close();
				}
			}
			return "success";
		}
	}

	private Timestamp getLastDay(Date t) {
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(t);
		calendar.add(Calendar.DATE, -1);
		Timestamp lastDate = new Timestamp(calendar.getTime().getTime());
		return lastDate;
	}
	
	private ArrayList<RealTimePrice> getPriceList(
			Timestamp firstTime, Timestamp tomorrow) {
		Connection conn = null;
		ArrayList<RealTimePrice> priceList = new ArrayList<RealTimePrice>();
		try {
			conn = DriverManager.getConnection(Utils.CONNECT_STRING);		
			String PriceSql = "select * from PH_PRICE where ELETYPE = 'res' AND TIME >= ? and Time <= ? ORDER BY TIME ASC NULLS LAST";
			PreparedStatement priceStatement = conn.prepareStatement(PriceSql);
			priceStatement.setTimestamp(1, firstTime);
			priceStatement.setTimestamp(2, tomorrow);
			ResultSet priceRet = priceStatement.executeQuery();

			if (priceRet != null) {
				while (priceRet.next()) {
					RealTimePrice tempPrice = new RealTimePrice();
					tempPrice.time = priceRet.getTimestamp(1);
					tempPrice.price = priceRet.getFloat(3);
					priceList.add(tempPrice);
				}
			}
		} catch (SQLException e) {

			e.printStackTrace();
		}finally{
		
		try {
			if(!conn.isClosed()){
				conn.close();
			}
		} catch (SQLException e) {

			e.printStackTrace();
		}
		}
		return priceList;
	}
	
	private Timestamp getLastUpdatePrice(Timestamp today)
			throws SQLException {
		Connection conn = DriverManager.getConnection(Utils.CONNECT_STRING);
		Timestamp firstTime = today;
		String latestPriceTimeSql = "select MAX(Time) as time from PH_PRICE where TIME <= ?";
		PreparedStatement lastPriceTimeStatement = conn
				.prepareStatement(latestPriceTimeSql);
		lastPriceTimeStatement.setTimestamp(1, today);
		ResultSet lastPriceTimeRet = lastPriceTimeStatement.executeQuery();
		if (lastPriceTimeRet != null) {
			while (lastPriceTimeRet.next()) {
				firstTime = lastPriceTimeRet.getTimestamp(1);
			}
		}
		if(!conn.isClosed()){
			conn.close();
		}
		return firstTime;
	}
}
