/*
 * Copyright The mod_cluster Project Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.jboss.modcluster;

import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.Locale;

/**
 * Utility class to be used in tests.
 *
 * @author Radoslav Husar
 */
public class TestUtils {

    private static final String ADVERTISE_INTERFACE_ADDRESS = System.getProperty("multicast.interface.address");
    private static final String ADVERTISE_INTERFACE_NAME = System.getProperty("multicast.interface.name");

    public static final String RFC_822_FMT = "EEE, d MMM yyyy HH:mm:ss Z";
    public static final DateFormat df = new SimpleDateFormat(RFC_822_FMT, Locale.US);
    public static final byte[] zeroMd5Sum = new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

    /**
     * Gets an interface to use for testing. First, attempts to resolve one of {@code multicast.interface.address} or
     * {@code multicast.interface.name} system properties, if unspecified, returns first multicast-enabled and
     * preferably non-loopback and non-point-to-point network interface.
     *
     * @return interface to be used for testing
     */
    public static NetworkInterface getAdvertiseInterface() throws SocketException, UnknownHostException {
        // First honor user configuration before picking automatically
        if (ADVERTISE_INTERFACE_ADDRESS != null && ADVERTISE_INTERFACE_NAME == null) {
            return NetworkInterface.getByInetAddress(InetAddress.getByName(ADVERTISE_INTERFACE_ADDRESS));
        } else if (ADVERTISE_INTERFACE_ADDRESS == null && ADVERTISE_INTERFACE_NAME != null) {
            return NetworkInterface.getByName(ADVERTISE_INTERFACE_NAME);
        } else if (ADVERTISE_INTERFACE_ADDRESS != null) {
            throw new IllegalStateException("Both -Dmulticast.interface.address and -Dmulticast.interface.name specified!");
        }

        // Automatically and deterministically find a first multicast-enabled with an IPv4 address
        // and preferably a non-loopback network interface.
        ArrayList<NetworkInterface> ifaces = Collections.list(NetworkInterface.getNetworkInterfaces());
        ifaces.sort(new Comparator<NetworkInterface>() {
            @Override
            public int compare(NetworkInterface o1, NetworkInterface o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });

        NetworkInterface electedIface = ifaces.get(0);
        for (NetworkInterface iface : ifaces) {
            if (iface.supportsMulticast() && hasIPv4Address(iface.getInetAddresses())) {
                if (!iface.isLoopback() && iface.isUp() && !iface.isPointToPoint()) {
                    electedIface = iface;
                    break;
                }
                electedIface = iface;
            }
        }
        System.out.println("Automatically using interface for testing: " + electedIface.getDisplayName() + ". To override use -Dmulticast.interface.address or -Dmulticast.interface.name!");

        return electedIface;
    }

    private static boolean hasIPv4Address(Enumeration<InetAddress> inetAddresses) {
        while (inetAddresses.hasMoreElements()) {
            if (inetAddresses.nextElement() instanceof Inet4Address) {
                return true;
            }
        }
        return false;
    }

    /**
     * Generates datagram packet content buffer including all fields as sent by native code.
     */
    public static byte[] generateAdvertisePacketData(Date date, int sequence, String server, String serverAddress) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");

        String rfcDate = df.format(date);

        md.update(zeroMd5Sum);
        digestString(md, rfcDate);
        digestString(md, String.valueOf(sequence));
        digestString(md, server);

        String data = String.format("HTTP/1.1 200 OK\r\nDate: %s\r\nSequence: %d\r\nDigest: %032x\r\nServer: %s\r\nX-Manager-Address: %s\r\n",
                rfcDate, sequence, new BigInteger(1, md.digest()), server, serverAddress);

        return data.getBytes();
    }

    /**
     * Utility method to digest {@link String}s.
     *
     * @param md {@link MessageDigest}
     * @param s  {@link String} to update the digest with
     */
    private static void digestString(MessageDigest md, String s) {
        int len = s.length();
        byte[] b = new byte[len];
        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);
            if (c < 127) {
                b[i] = (byte) c;
            } else {
                b[i] = '?';
            }
        }
        md.update(b);
    }

    // Utility class
    private TestUtils() {
    }
}
