/*
 * ao-messaging-tcp-client - Client for asynchronous bidirectional messaging over TCP sockets.
 * Copyright (C) 2014, 2015, 2016, 2019, 2020, 2021, 2022  AO Industries, Inc.
 *     support@aoindustries.com
 *     7262 Bull Pen Cir
 *     Mobile, AL 36695
 *
 * This file is part of ao-messaging-tcp-client.
 *
 * ao-messaging-tcp-client is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ao-messaging-tcp-client is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with ao-messaging-tcp-client.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.aoapps.messaging.tcp.client;

import com.aoapps.concurrent.Callback;
import com.aoapps.concurrent.Executors;
import com.aoapps.hodgepodge.io.stream.StreamableInput;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoapps.lang.Throwables;
import com.aoapps.messaging.base.AbstractSocketContext;
import com.aoapps.messaging.tcp.TcpSocket;
import com.aoapps.security.Identifier;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Client component for bi-directional messaging over TCP.
 */
public class TcpSocketClient extends AbstractSocketContext<TcpSocket> {

  private static final Logger logger = Logger.getLogger(TcpSocketClient.class.getName());

  private static final boolean KEEPALIVE = true;

  private static final boolean SOCKET_SO_LINGER_ENABLED = true;
  private static final int SOCKET_SO_LINGER_SECONDS = 15;

  private static final boolean TCP_NO_DELAY = true;

  private static final int CONNECT_TIMEOUT = 15 * 1000;

  private final Executors executors = new Executors();

  @Override
  public void close() {
    try {
      super.close();
    } finally {
      executors.close();
    }
  }

  /**
   * Asynchronously connects.
   */
  @SuppressWarnings({"UseSpecificCatch", "TooBroadCatch", "AssignmentToCatchBlockParameter"})
  public void connect(
    SocketAddress endpoint,
    Callback<? super TcpSocket> onConnect,
    Callback<? super Throwable> onError
  ) {
    executors.getUnbounded().submit(() -> {
      try {
        Socket socket = new Socket();
        socket.setKeepAlive(KEEPALIVE);
        socket.setSoLinger(SOCKET_SO_LINGER_ENABLED, SOCKET_SO_LINGER_SECONDS);
        socket.setTcpNoDelay(TCP_NO_DELAY);
        long connectTime = System.currentTimeMillis();
        socket.connect(endpoint, CONNECT_TIMEOUT);
        logger.log(Level.FINEST, "Got connection");
        boolean successful = false;
        try {
          StreamableInput in = new StreamableInput(socket.getInputStream());
          logger.log(Level.FINEST, "Got in");
          StreamableOutput out = new StreamableOutput(socket.getOutputStream());
          logger.log(Level.FINEST, "Got out");
          Identifier id = new Identifier(in.readLong(), in.readLong());
          logger.log(Level.FINEST, "Got id = {0}", id);
          TcpSocket tcpSocket = new TcpSocket(
            TcpSocketClient.this,
            id,
            connectTime,
            socket,
            in,
            out
          );
          logger.log(Level.FINEST, "Adding socket");
          addSocket(tcpSocket);
          if (onConnect != null) {
            logger.log(Level.FINE, "Calling onConnect: {0}", tcpSocket);
            try {
              onConnect.call(tcpSocket);
            } catch (ThreadDeath td) {
              throw td;
            } catch (Throwable t) {
              logger.log(Level.SEVERE, null, t);
            }
          } else {
            logger.log(Level.FINE, "No onConnect: {0}", tcpSocket);
          }
          successful = true;
        } finally {
          if (!successful) {
            try {
              socket.close();
            } catch (ThreadDeath td) {
              throw td;
            } catch (Throwable t) {
              logger.log(Level.SEVERE, null, t);
            }
          }
        }
      } catch (Throwable t0) {
        if (onError != null) {
          logger.log(Level.FINE, "Calling onError", t0);
          try {
            onError.call(t0);
          } catch (ThreadDeath td) {
            t0 = Throwables.addSuppressed(td, t0);
            assert t0 == td;
          } catch (Throwable t2) {
            logger.log(Level.SEVERE, null, t2);
          }
        } else {
          logger.log(Level.FINE, "No onError", t0);
        }
        if (t0 instanceof ThreadDeath) {
          throw (ThreadDeath)t0;
        }
      }
    });
  }
}
