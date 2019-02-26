package org.openplacereviews.opendb.ops;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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

	public synchronized void addDelInfo(int ind, OpOperation op, OpBlockChain blc) {
		DeleteInfo i = new DeleteInfo();
		i.blc = blc;
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
	
	public Iterator<Map.Entry<OpOperation, OpBlockChain>> getDelInfoIterator(int ind) {
		if(deletedObjs == null || deletedObjs.size() <= ind || deletedObjs.get(ind) == null) {
			return new Iterator<Map.Entry<OpOperation,OpBlockChain>>() {
				@Override
				public Entry<OpOperation, OpBlockChain> next() {
					return null;
				}
				@Override
				public boolean hasNext() {
					return false;
				}
			};
		}
		DeleteInfo odi = deletedObjs.get(ind);
		return new Iterator<Map.Entry<OpOperation,OpBlockChain>>() {
			DeleteInfo next = odi;
			@Override
			public Entry<OpOperation, OpBlockChain> next() {
				DeleteInfo t = next;
				next = next.otherDeleteInfo;
				return new Entry<OpOperation, OpBlockChain>() {
					@Override
					public OpBlockChain setValue(OpBlockChain value) {
						throw new UnsupportedOperationException();
					}
					
					@Override
					public OpBlockChain getValue() {
						return t.blc;
					}
					
					@Override
					public OpOperation getKey() {
						return t.op;
					}
				};
			}
			@Override
			public boolean hasNext() {
				return next != null;
			}
		};
		
	}

	private static class DeleteInfo {
		OpOperation op;
		OpBlockChain blc;

		// simple linked list
		DeleteInfo otherDeleteInfo;
	}

}