/*
 * Copyright (C) 2016 BROADSoftware
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kappaware.king.nju;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpMethod;

public class JettyClient {

	static public void main(String[] argv) throws Exception {
		boolean async = true;
		HttpClient httpClient = new HttpClient();
		httpClient.setFollowRedirects(false);
		httpClient.setMaxConnectionsPerDestination(1);
		httpClient.setStrictEventOrdering(true);

		httpClient.start();

		// WARNING: In asynchronous mode, more than 1024 pending request by destination leads an error.
		// Pb (Bad jetty design ?) is that this is only notified by the callback. So, too late.
		// (In fact, in such case, notification occurs in the calling thread, which may give an opportunity to queue the request).
		
		for (int cpt = 0; cpt < 1000; cpt++) {
			String data = String.format("Message #%d# ", cpt);
			Request request = httpClient.newRequest("http://127.0.0.1:2016/main/xx?p1=aa&p2=bb&p2=cc");
			request.method(HttpMethod.PUT).agent("Java client").content(new StringContentProvider(data), "text/plain").attribute("cpt", cpt);
			System.out.printf("Send #%d\n", cpt);
			if (async) {
				request.send((result) -> {
					System.out.printf("Async response cpt=%d status=%d\n",  (int)result.getRequest().getAttributes().get("cpt"), result.getResponse().getStatus());
					if(result.isFailed()) {
						throw new RuntimeException(result.getRequestFailure());
					}
				});
			} else {
				ContentResponse response = request.send();
				System.out.printf("Sync response cpt=%d status=%d\n", (int)request.getAttributes().get("cpt"), response.getStatus());
				if(response.getStatus() != 200) {
					throw new RuntimeException("Invalid status");
				}
			}
		}
	}
}
