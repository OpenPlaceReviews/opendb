package org.openplacereviews.opendb.ops;

import java.util.ArrayList;
import java.util.List;

public class SimulateSuperblockCompactSequences {

	
	// !! COMPACT_COEF >= 1 / SUBCHAIN_SIZE_TO_COMPACT !! 
	private static final double COMPACT_COEF = 1f;
	
	private static int SIMULATION_SIZE = 10000;
	private static int PRINT_ITERATION = 100;
	
	private static int compacted = 0;
	private static int merged = 0;

	public static void main(String[] args) {
		ArrayList<Integer> list = new ArrayList<>();
		int i = 0;
		while (i++ < SIMULATION_SIZE) {
			list.add(0, 1);
			compactIter(list, i);
			compactIter(list, i);
		}
		System.out.println(String.format("Compacted %d, merged blocks %d, current length %d", compacted, merged, list.size()));
	}


	private static void compactIter(ArrayList<Integer> list, int i) {
		boolean print = i % PRINT_ITERATION == 0;
		if (print) {
			System.out.print(i + ". " + list);
		}
		int mg = compact(list, 0);
		if(mg > 0) {
			if(print ) {
				System.out.println(" -> " + list);
			}
			compacted ++;
			merged += mg;
		} else {
			if (print) {
				System.out.println();
			}
		}
	}

	
	private static int compact(List<Integer> list, int ind) {
		if (list.size() > ind + 1) {
			int compacted = compact(list, ind + 1);
			if(compacted == 0 && ind > 0) {
				if (COMPACT_COEF * list.get(ind - 1) + list.get(ind) > list.get(ind + 1 )) {
					return mergeWithParent(list, ind);
				}
			}
			return compacted;
		}
		return 0;
	}

	private static int mergeWithParent(List<Integer> list, int ind) {
		Integer ni = list.remove((int) ind);
		int mergedBlocks = list.get(ind) + ni;
		list.set(ind, list.get(ind) + ni);
		return mergedBlocks;
	}
}
