package org.openplacereviews.places;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.xmlpull.v1.XmlPullParserException;

import net.osmand.IProgress;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Node;
import net.osmand.osm.io.IOsmStorageFilter;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.util.MapUtils;

public class TestGitPlaces {
	private static final String ROOT_FOLDER = "../../../../opr-data/";
	private static final String AMENITIES_FILE = ROOT_FOLDER + "amenities.osm.gz";
	private static final String GIT_REPO = ROOT_FOLDER + "git-repo";
	
	public static void main(String[] args) throws GitAPIException, IOException, XmlPullParserException {
		File gitDir = new File(GIT_REPO);
		Git git ;
		if (!gitDir.exists() || ((git = Git.open(gitDir)) == null)) {
			System.out.println("Initialize git repo");
			git = Git.init().setDirectory(gitDir).call();
		}
		Git gitObj = git ;
		
		Status status = git.status().call();
		System.out.println(status.isClean());
		
		OsmBaseStorage osmbs = new OsmBaseStorage();
		long startTime = System.currentTimeMillis();
		osmbs.getFilters().add(new IOsmStorageFilter() {
			int count = 0;
			@Override
			public boolean acceptEntityToLoad(OsmBaseStorage storage, EntityId entityId, Entity entity) {
				Node n = (Node) entity;
				String fullLocation = MapUtils.createShortLinkString(n.getLatitude(), n.getLongitude(), 16);
				File placeDir = new File(gitDir, fullLocation.substring(0, 4));
				if(!placeDir.exists()) {
					placeDir.mkdirs();
				}
				File placeFile = new File(placeDir, fullLocation + " " + n.getId());
				try {
					FileOutputStream fous = new FileOutputStream(placeFile);
					fous.write(n.toString().getBytes());
					fous.close();
					gitObj.add().addFilepattern(placeDir.getName() + "/" + placeFile.getName()).call();
					gitObj.commit().setMessage("Commit " + n.getId()).call();
				} catch (Exception e) {
					throw new IllegalStateException("error", e);
				}
				if(count++ > 10000) {
					throw new IllegalStateException("stop");
				} else if(count % 100 == 0) {
					System.out.println(String.format("%d places processed, %d seconds past",
							count, (System.currentTimeMillis() - startTime) / 1000));
				}
				return false;
			}
			
		});
		FileInputStream is = new FileInputStream(AMENITIES_FILE);
		osmbs.parseOSM(new GZIPInputStream(is), IProgress.EMPTY_PROGRESS, is, false);
		
		
	}
}
