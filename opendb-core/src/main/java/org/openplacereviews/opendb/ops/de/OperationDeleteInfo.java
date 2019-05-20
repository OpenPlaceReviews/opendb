package org.openplacereviews.opendb.ops.de;

import java.util.List;

import org.openplacereviews.opendb.ops.OpOperation;

public class OperationDeleteInfo {
	public OpOperation op;
	public boolean create;
	public boolean[] deletedObjects;
	public List<String> deletedOpHashes;
}