package com.techwalk.cassandra.Techwalk_Cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

/**
 * 
 * @description: This class is used to connect Cassandra database.
 * @author techwalk
 *
 */
public class CassandraConnection {

	private Cluster cluster;
	private Session session;

	/**
	 * Connect to Cassandra Cluster specified by provided node IP address and
	 * port number.
	 *
	 * @param node
	 *            Cluster node IP address.
	 * @param port
	 *            Port of cluster host.
	 */
	public void connect(final String node, final int port) {
		this.cluster = Cluster.builder().addContactPoint(node).withPort(port).build();
		final Metadata metadata = cluster.getMetadata();
		System.out.println("Connected to cluster: " + metadata.getClusterName());
		for (final Host host : metadata.getAllHosts()) {
			System.out.println(
					"Host information: " + host.getDatacenter() + " " + host.getAddress() + " " + host.getDatacenter());
		}
		session = cluster.connect();
	}

	public Session getSession() {
		return this.session;
	}

	public void close() {
		cluster.close();
	}

}
