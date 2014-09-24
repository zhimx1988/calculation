package elec.calculation;

import java.sql.Timestamp;
import java.util.ArrayList;

public class RealTimePrice implements Comparable<RealTimePrice> {

	public Timestamp time;
	public float price;

	public int compareTo(RealTimePrice o) {
		// TODO Auto-generated method stub
		return this.time.compareTo(o.time);
	}

	public static RealTimePrice lower_bound(ArrayList<RealTimePrice> list,
			RealTimePrice target) {
		int begin = 0, end = list.size() - 1;
		int ans = -1;
		while (begin <= end) {
			int mid = begin + (end - begin) / 2;
			int comRet = target.compareTo(list.get(mid));
			if (comRet > 1) {
				end = mid - 1;
			} else {
				begin = mid + 1;
				ans = mid;
			}
		}
		if (ans < 0)
			return null;
		return list.get(ans);

	}

	public static RealTimePrice lower_bound(ArrayList<RealTimePrice> list,
			Timestamp target) {
		int begin = 0, end = list.size() - 1;
		int ans = -1;
		while (begin <= end) {
			int mid = begin + (end - begin) / 2;
			int comRet = target.compareTo(list.get(mid).time);
			if (comRet == -1) {
				end = mid - 1;
			} else {
				begin = mid + 1;
				ans = mid;
			}
		}
		if (ans < 0)
			return null;
		return list.get(ans);

	}
}
