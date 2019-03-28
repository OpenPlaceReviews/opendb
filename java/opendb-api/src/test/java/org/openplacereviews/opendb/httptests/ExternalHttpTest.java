package org.openplacereviews.opendb.httptests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.junit.Test;

public class ExternalHttpTest {

	protected static String LOCATION = "http://localhost:6463/api/"; 
	protected static String AUTH_ADMIN_LOGIN = "auth/admin-login";
	protected static String AUTH_SIGNUP = "auth/signup";
	protected static String AUTH_LOGIN = "auth/login";
	protected static String MGMT_TOGGLE_PAUSE = "mgmt/toggle-pause";
	protected static String MGMT_BOOTSTRAP = "mgmt/bootstrap";
	protected static String MGMT_CREATE_BLOCK = "mgmt/create";
	protected static String MGMT_QUEUE_CLEAR = "mgmt/queue-clear";
	protected static String MGMT_LOGS_CLEAR = "mgmt/logs-clear";
	
	private static String USERNAME = "openplacereviews:test_1";
	private static String USER_PWD = "base64:PKCS#8:MD4CAQAwEAYHKoZIzj0CAQYFK4EEAAoEJzAlAgEBBCAOpUDyGrTPRPDQRCIRXysxC6gCgSTiNQ5nVEjhvsFITA==";
	
	@Test
	public void testSimpleWebOps() throws IOException {
		CloseableHttpClient client = HttpClients.createDefault();
		adminLogin(client);
//		executeMgmtOp(client, MGMT_BOOTSTRAP);
//		executeMgmtOp(client, MGMT_CREATE_BLOCK);
		
//		executeMgmtOp(client, MGMT_TOGGLE_PAUSE);
		final int userInd = (int) ((System.currentTimeMillis() % 10000l) * 10000l);
		userSignup(client, "user_"+userInd, "supersecretepassword");
		executeMgmtOp(client, MGMT_CREATE_BLOCK);
		
		for (int i = 1; i < 15; i++) {
			// userSignup(client, "user_"+userInd++, "supersecretepassword");
			// userSignup(client, "user_"+userInd++, "supersecretepassword");
			userLogin(client, "user_" + userInd + ":" + (userInd+i), "supersecretepassword");
			executeMgmtOp(client, MGMT_CREATE_BLOCK);
		}
		
		
//		executeMgmtOp(client, MGMT_TOGGLE_PAUSE);
//		executeMgmtOp(client, MGMT_QUEUE_CLEAR);
		client.close();
	}

	private void executeMgmtOp(CloseableHttpClient client, String op) throws IOException {
		HttpPost post = new HttpPost(LOCATION + op);
		executePost(client, post);
	}

	private void executePost(CloseableHttpClient client, HttpPost post) throws IOException,
			ClientProtocolException {
		CloseableHttpResponse resp = client.execute(post);
		System.out.println("Execute " + post.getURI() +  " " + resp.getStatusLine().getStatusCode());
		resp.close();
	}

	private void adminLogin(CloseableHttpClient client) throws IOException{
		HttpPost post = new HttpPost(LOCATION + AUTH_ADMIN_LOGIN);
		List<NameValuePair> params = new ArrayList<NameValuePair>();
		params.add(new BasicNameValuePair("name", USERNAME));
		params.add(new BasicNameValuePair("pwd", USER_PWD));
		post.setEntity(new UrlEncodedFormEntity(params));
		executePost(client, post);
	}
	
	private void userSignup(CloseableHttpClient client, String name, String pwd) throws IOException{
		HttpPost post = new HttpPost(LOCATION + AUTH_SIGNUP);
		List<NameValuePair> params = new ArrayList<NameValuePair>();
		params.add(new BasicNameValuePair("name", name));
		params.add(new BasicNameValuePair("pwd", pwd));
		post.setEntity(new UrlEncodedFormEntity(params));
		executePost(client, post);
	}
	
	private void userLogin(CloseableHttpClient client, String name, String pwd) throws IOException{
		HttpPost post = new HttpPost(LOCATION + AUTH_LOGIN);
		List<NameValuePair> params = new ArrayList<NameValuePair>();
		params.add(new BasicNameValuePair("name", name));
		params.add(new BasicNameValuePair("pwd", pwd));
		post.setEntity(new UrlEncodedFormEntity(params));
		executePost(client, post);
	}
	
}
