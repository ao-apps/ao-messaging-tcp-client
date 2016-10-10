/*
 * ao-messaging - Asynchronous bidirectional messaging over various protocols.
 * Copyright (C) 2014, 2015, 2016  AO Industries, Inc.
 *     support@aoindustries.com
 *     7262 Bull Pen Cir
 *     Mobile, AL 36695
 *
 * This file is part of ao-messaging.
 *
 * ao-messaging is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ao-messaging is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with ao-messaging.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.aoindustries.messaging.tcp.client;

import com.aoindustries.io.CompressedDataInputStream;
import com.aoindustries.io.CompressedDataOutputStream;
import com.aoindustries.messaging.base.AbstractSocketContext;
import com.aoindustries.messaging.tcp.TcpSocket;
import com.aoindustries.security.Identifier;
import com.aoindustries.util.concurrent.Callback;
import com.aoindustries.util.concurrent.Executors;
import java.net.Socket;
import java.net.SocketAddress;

/**
 * Client component for bi-directional messaging over TCP.
 */
public class TcpSocketClient extends AbstractSocketContext<TcpSocket> {

	private static final boolean DEBUG = false;

	private static final boolean KEEPALIVE = true;

	private static final boolean SOCKET_SO_LINGER_ENABLED = true;
	private static final int SOCKET_SO_LINGER_SECONDS = 15;

	private static final boolean TCP_NO_DELAY = true;

	private static final int CONNECT_TIMEOUT = 15 * 1000;

	private final Executors executors = new Executors();

	public TcpSocketClient() {
	}

	@Override
	public void close() {
		try {
			super.close();
		} finally {
			executors.dispose();
		}
	}

	/**
	 * Asynchronously connects.
	 */
	public void connect(
		final SocketAddress endpoint,
		final Callback<? super TcpSocket> onConnect,
		final Callback<? super Exception> onError
	) {
		executors.getUnbounded().submit(
			new Runnable() {
				@Override
				public void run() {
					try {
						Socket socket = new Socket();
						socket.setKeepAlive(KEEPALIVE);
						socket.setSoLinger(SOCKET_SO_LINGER_ENABLED, SOCKET_SO_LINGER_SECONDS);
						socket.setTcpNoDelay(TCP_NO_DELAY);
						long connectTime = System.currentTimeMillis();
						socket.connect(endpoint, CONNECT_TIMEOUT);
						if(DEBUG) System.out.println("DEBUG: TcpSocketClient: connect: got connection");
						boolean successful = false;
						try {
							CompressedDataInputStream in = new CompressedDataInputStream(socket.getInputStream());
							if(DEBUG) System.out.println("DEBUG: TcpSocketClient: connect: got in");
							CompressedDataOutputStream out = new CompressedDataOutputStream(socket.getOutputStream());
							if(DEBUG) System.out.println("DEBUG: TcpSocketClient: connect: got out");
							Identifier id = new Identifier(in.readLong(), in.readLong());
							if(DEBUG) System.out.println("DEBUG: TcpSocketClient: connect: got id=" + id);
							TcpSocket tcpSocket = new TcpSocket(
								TcpSocketClient.this,
								id,
								connectTime,
								socket,
								in,
								out
							);
							if(DEBUG) System.out.println("DEBUG: TcpSocketClient: connect: adding socket");
							addSocket(tcpSocket);
							if(onConnect!=null) {
								if(DEBUG) System.out.println("DEBUG: TcpSocketClient: connect: calling onConnect");
								onConnect.call(tcpSocket);
							}
							successful = true;
						} finally {
							if(!successful) {
								socket.close();
							}
						}
					} catch(Exception exc) {
						if(onError!=null) {
							if(DEBUG) System.out.println("DEBUG: TcpSocketClient: connect: calling onError");
							onError.call(exc);
						}
					}
				}
			}
		);
	}
}
