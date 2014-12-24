package com.ruc.xx427.optimizer.model;

import java.util.Comparator;

/**
 * 
 * Description: to get slot tracks based on the sorted progress 
 *
 * @author Juwei
 * @date 2014Äê12ÔÂ23ÈÕ
 *
 */
public class AllocatedSlotsComparator implements Comparator<AllocatedSlots>{

	@Override
	public int compare(AllocatedSlots o1, AllocatedSlots o2) {
		if (o1.getProgressOfRunningSlots()>o2.getProgressOfRunningSlots())
			return 1;
		return -1;
	}

}
