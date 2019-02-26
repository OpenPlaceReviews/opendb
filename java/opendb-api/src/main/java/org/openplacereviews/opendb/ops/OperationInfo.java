package org.openplacereviews.opendb.ops;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class OperationInfo {
	private final OpOperation op;
	private final OpBlockChain blc;
	private final int blockId;
	private List<DeleteInfo> deletedObjs;
	
	public OperationInfo(OpOperation op, OpBlockChain b, int blockId) {
		this.op = op;
		this.blc = b;
		this.blockId = blockId;
	}
	
	public OpOperation getOp() {
		return op;
	}
	
	public OpBlockChain getBlc() {
		return blc;
	}
	
	public int getBlockId() {
		return blockId;
	}

	public synchronized void addDelInfo(int ind, OpOperation op, int blockId, String blockHash) {
		DeleteInfo i = new DeleteInfo();
		i.blockId = blockId;
		i.blockHash = blockHash;
		i.op = op;
		if (deletedObjs == null) {
			deletedObjs = new ArrayList<DeleteInfo>(op.getNew().size());
			for (int k = 0; k < op.getNew().size(); k++) {
				deletedObjs.add(null);
			}
		}
		DeleteInfo odi = deletedObjs.get(ind);
		if (odi == null) {
			deletedObjs.set(ind, i);
		} else {
			while (odi.otherDeleteInfo != null) {
				odi = odi.otherDeleteInfo;
			}
			odi.otherDeleteInfo = i;
		}
	}
	
	public Iterator<DeleteInfo> getDelInfoIterator(int ind) {
		if(deletedObjs == null || deletedObjs.size() <= ind || deletedObjs.get(ind) == null) {
			return new Iterator<DeleteInfo>() {
				@Override
				public DeleteInfo next() {
					return null;
				}
				@Override
				public boolean hasNext() {
					return false;
				}
			};
		}
		DeleteInfo odi = deletedObjs.get(ind);
		return new Iterator<DeleteInfo>() {
			DeleteInfo next = odi;
			@Override
			public DeleteInfo next() {
				DeleteInfo t = next;
				next = next.otherDeleteInfo;
				return t;
			}
			@Override
			public boolean hasNext() {
				return next != null;
			}
		};
		
	}

	public static class DeleteInfo {
		int blockId;
		String blockHash;
		OpOperation op;

		// simple linked list
		DeleteInfo otherDeleteInfo;
	}

}