package org.openplacereviews.opendb.ops;

import java.util.ArrayList;
import java.util.List;

public class SimulateSuperblockCompactSequences {

	
	// !! COMPACT_COEF >= 1 / SUBCHAIN_SIZE_TO_COMPACT !! 
	private static final double COMPACT_COEF = 0.5f;
	private static final int MAX_BLOCK_SIZE = 32000;
	private static int SUBCHAIN_SIZE_TO_COMPACT = 2; 
	
	
	private static int SIMULATION_SIZE = 100000;
	private static int PRINT_ITERATION = 1000
			;
	
	private static int compacted = 0;
	private static int merged = 0;

	public static void main(String[] args) {
		ArrayList<Integer> list = new ArrayList<>();
		int i = 0;
		while (i++ < SIMULATION_SIZE) {
			list.add(0, 1);
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
		System.out.println(String.format("Compacted %d, merged blocks %d, current length %d", compacted, merged, list.size()));
	}

	
	private static int compact(List<Integer> list, int l) {
		if (list.size() > l + SUBCHAIN_SIZE_TO_COMPACT) {
			int blockSize = list.get(l + SUBCHAIN_SIZE_TO_COMPACT);
			int sumSubchainSize = 0;
			for(int k = 0; k < SUBCHAIN_SIZE_TO_COMPACT; k++) {
				sumSubchainSize += list.get(k + l);
			}
			if (COMPACT_COEF * sumSubchainSize >= blockSize && blockSize < MAX_BLOCK_SIZE) {
				int mergedBlocks = mergeWithParent(list, l + SUBCHAIN_SIZE_TO_COMPACT - 1);
				return mergedBlocks;
			} else {
				return compact(list, l + 1);
			}
		}
		return 0;
	}

	private static int mergeWithParent(List<Integer> list, int l) {
		Integer ni = list.remove((int) l);
		int mergedBlocks = list.get(l) + ni;
		list.set(l, list.get(l) + ni);
		return mergedBlocks;
	}
}
