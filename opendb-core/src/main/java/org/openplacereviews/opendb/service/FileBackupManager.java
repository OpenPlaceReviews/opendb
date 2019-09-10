package org.openplacereviews.opendb.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.zip.GZIPOutputStream;

@Service
public class FileBackupManager {

	protected static final Log LOGGER = LogFactory.getLog(FileBackupManager.class);
	private static final String SEPARATOR_STRING = "\n---------------------\n";

	private File mainDirectory;
	
	@Value("${opendb.files-backup.file-prefix:}")
	private String FILE_PREFIX;
	
	@Value("${opendb.db.file-max-blocks-size:1000}")
	private int MAX_FILE_SIZE;
	
	@Value("${opendb.db.dir-max-size:100000}")
	private int MAX_DIR_SIZE;
	
	private boolean enabled = false;
	

	@Autowired
	private JsonFormatter formatter;

	@Autowired
	private SettingsManager settingsManager;
	
	public void init() {
		if(getDirectory().length() > 0) {
			mainDirectory = new File(getDirectory());
			mainDirectory.mkdirs();
		}
		if(mainDirectory.exists()) {
			enabled = true;
		}
		if (FILE_PREFIX == null) {
			FILE_PREFIX = "";
		}
	}

	public synchronized void insertBlock(OpBlock opBlock) {
		if(enabled) {
			String fs = formatter.fullObjectToJson(opBlock);
			
			int blockId = opBlock.getBlockId();
			int dirId = blockId / MAX_DIR_SIZE;
			File parent = new File(mainDirectory, dirId+"");
			parent.mkdirs();
			
			int rid = blockId % MAX_DIR_SIZE;
			int minBlockId = (rid / MAX_FILE_SIZE) * MAX_FILE_SIZE;
			int maxBlockId = (rid / MAX_FILE_SIZE + 1) * MAX_FILE_SIZE;
			String file = FILE_PREFIX + minBlockId + "-"+maxBlockId +".gz";
			File f = new File(parent, file);
			try (GZIPOutputStream gout = new GZIPOutputStream(new FileOutputStream(f, true)) ){
				Writer w = new OutputStreamWriter(gout);
				w.write(SEPARATOR_STRING);
				w.write(fs);
				w.close();
			} catch (IOException e) {
				LOGGER.error(String.format("Error writing block '%s:%d' to '%s'", 
						opBlock.getRawHash(), opBlock.getBlockId(), f.getName()));
			}
		}
		
	}

	private String getDirectory() {
		return settingsManager.OPENDB_FILE_BACKUP_DIRECTORY.get();
	}
	
}
