package org.springframework.data.cassandra.examples.core.net;

import java.net.InetSocketAddress;

@SuppressWarnings("unused")
public abstract class NetworkUtils {

	public static InetSocketAddress newSocketAddress(String hostname, int port) {
		return new InetSocketAddress(hostname, port);
	}
}
