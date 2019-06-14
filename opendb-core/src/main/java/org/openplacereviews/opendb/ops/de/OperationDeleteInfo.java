package org.openplacereviews.opendb.ops.de;

import org.openplacereviews.opendb.ops.OpOperation;

import java.util.List;

@Deprecated
public class OperationDeleteInfo {
	public OpOperation op;
	public boolean create;
	public boolean[] deletedObjects;
	public List<String> deletedOpHashes;
}