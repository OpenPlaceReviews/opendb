package org.openplacereviews.places;

import java.io.File;
import java.io.IOException;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.api.errors.GitAPIException;

public class TestGitPlaces {

	public static void main(String[] args) throws GitAPIException, IOException {
		File gitDir = new File("osm/git-repo");
		Git git ;
		if (!gitDir.exists() || ((git = Git.open(gitDir)) == null)) {
			System.out.println("Initialize git repo");
			git = Git.init().setDirectory(gitDir).call();
		}
		
		Status status = git.status().call();
		System.out.println(status.isClean());
	}
}
