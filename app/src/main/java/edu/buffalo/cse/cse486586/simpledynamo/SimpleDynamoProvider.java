package edu.buffalo.cse.cse486586.simpledynamo;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import android.content.Context;
import android.database.MatrixCursor;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

/* Simple Dynamo - Created by Ranganath Sundar*/


public class SimpleDynamoProvider extends ContentProvider {

    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    static ArrayList<String> REMOTE_PORTS = new ArrayList<String>();
    static ArrayList<String> NEWREMOTE_PORTS = new ArrayList<String>();
    static final int SERVER_PORT = 10000;
    static int sequence = 0;
    static ArrayList<String> aliveNodes = new ArrayList<String>();
    static ArrayList<String> aliveNodesList = new ArrayList<String>();
    String queryResponse = "";
    int responseCount = 0;
    String succPortHash = "";
    String myPortHash = "";
    static HashMap<String, String> nodeLookup = new HashMap<String, String>();
    static HashMap<String, String> updateLookup = new HashMap<String, String>();
    String predPortHash = "";
    String myPort = "";
    String port = "";
    String succPort = "";
    String predPort = "";
    String currentHash = "";
    String queryMsg = null;
    boolean flag = false;
    String keyDelete = "";
    String[] columns = {"key", "value"};
    MatrixCursor matrixCursor;
    private Object lock1 = new Object();
    private Object lock2 = new Object();
    private Object lock3 = new Object();
    int count = 0;
    String alive= "";
    String failedNode = "none";
    String recoverResponse= "";
    int aliveCount = 0;
    int recoverCount = 0;

    private HashMap<String, String> provider = new HashMap<String, String>();
    private HashMap<String, String> pingCount = new HashMap<String, String>();

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        String keyDelete = selection;
        if (keyDelete.equals("@")) {
            provider.clear();

        }
        else if (keyDelete.equals("*")) {
            try {
                TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
                String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
                final String myPort = String.valueOf((Integer.parseInt(portStr)));
                currentHash = genHash(myPort);
                String msgDelete = myPort + "%%" + "Asterisk Delete" + "%%" + "@";
                Socket socket;
                OutputStream outputStream;
                DataOutputStream dataOutputStream;
                for (int i = REMOTE_PORTS.size() - 1; i >= 0; i--) {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(REMOTE_PORTS.get(i)));
                    outputStream = socket.getOutputStream();
                    dataOutputStream = new DataOutputStream(outputStream);
                    dataOutputStream.write(msgDelete.getBytes());
                    socket.close();
                }
            } catch (Exception e) {
                e.getMessage();
            }
        }
        else {

            try {
                if (provider.containsKey(keyDelete)) {
                    Log.i(TAG, "Enters IF case in delete ELSE");
                    String value = provider.remove(keyDelete);

                }

                else {

                    try {
                        String delForward = myPort + "%%" + "KeyDelete" + "%%" + keyDelete;
                        Socket socket;
                        OutputStream outputStream;
                        DataOutputStream dataOutputStream;
                        //for (int i = REMOTE_PORT.length - 1; i >= 0; i--) {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(succPort));
                        outputStream = socket.getOutputStream();
                        dataOutputStream = new DataOutputStream(outputStream);
                        dataOutputStream.write(delForward.getBytes());
                        socket.close();
                    } catch (Exception e) {
                        e.getMessage();
                    }


                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return 0;

    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        synchronized (lock2) {
            try {

                String key = values.getAsString("key");
                String value = values.getAsString("value");
                Log.i(TAG, "The key is:" + key);

                String[] keypart = key.split("::");
                if (keypart.length > 1 && keypart[1].equals("Replicate")) {
                    String key_in = keypart[0];
                    Log.i(TAG, "The key is:" + key_in + " value is: " + value);

                    if (provider.get(key_in)==null) {
                        Log.i(TAG, "NO existing value for key..inserting");
                        provider.put(key_in, value);
                    }
                    else if(provider.get(key_in)!=null)
                    {
                        Log.i(TAG, "Existing value found for key..removing first and inserting new value");
                        provider.remove(key_in);
                        provider.put(key_in,value);
                    }
                    Log.i(TAG, "Replication ho gaya");
                } else {
                    String keyNode = findPosition(key);
                    Log.i(TAG, "Enters ELSE case in insert");
                    Log.i(TAG, "My port is " + myPort + "and position is :" + keyNode);
                    if (myPort.equals(keyNode)) {
                        Log.i(TAG, "Enters IF case in ELSE for insert");


                        if (provider.get(key)==null) {
                            Log.i(TAG, "NO existing value for key..inserting");
                            provider.put(key, value);
                        }
                        else if(provider.get(key)!=null)
                        {
                            Log.i(TAG, "Existing value found for key..removing first and inserting new value");
                            provider.remove(key);
                            provider.put(key,value);
                        }

                        String msgForward = myPort + "%%" + "Replicate" + "%%" + key + "%%" + value;
                        for (int i = NEWREMOTE_PORTS.size() - 1; i >= 0; i--) {
                            if (succPort.equals(NEWREMOTE_PORTS.get(i))) {

                                Log.i(TAG, "NEWREMOTE_PORTS size is" + NEWREMOTE_PORTS.size());

                                if (i == NEWREMOTE_PORTS.size() - 1) {
                                    Log.i(TAG, "Value of i is: " + i);
                                    Socket socket;
                                    OutputStream outputStream;
                                    DataOutputStream dataOutputStream;
                                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            Integer.parseInt(succPort));
                                    Log.i(TAG, "Sending Replicate to : " + succPort);
                                    outputStream = socket.getOutputStream();
                                    dataOutputStream = new DataOutputStream(outputStream);
                                    dataOutputStream.write(msgForward.getBytes());
                                    socket.close();

                                    Socket newsocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            Integer.parseInt(NEWREMOTE_PORTS.get(0)));
                                    Log.i(TAG, "Sending Replicate to : " + NEWREMOTE_PORTS.get(0));
                                    outputStream = newsocket.getOutputStream();
                                    dataOutputStream = new DataOutputStream(outputStream);
                                    dataOutputStream.write(msgForward.getBytes());
                                    newsocket.close();

                                } else {
                                    Socket socket;
                                    OutputStream outputStream;
                                    DataOutputStream dataOutputStream;
                                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            Integer.parseInt(succPort));
                                    Log.i(TAG, "Sending Replicate to : " + NEWREMOTE_PORTS.get(i));
                                    outputStream = socket.getOutputStream();
                                    dataOutputStream = new DataOutputStream(outputStream);
                                    dataOutputStream.write(msgForward.getBytes());
                                    socket.close();

                                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            Integer.parseInt(NEWREMOTE_PORTS.get(i + 1)));
                                    Log.i(TAG, "Sending Replicate to : " + NEWREMOTE_PORTS.get(i + 1));
                                    outputStream = socket.getOutputStream();
                                    dataOutputStream = new DataOutputStream(outputStream);
                                    dataOutputStream.write(msgForward.getBytes());
                                    socket.close();
                                }
                            }
                        }
                    } else {
                        String msgForward = myPort + "%%" + "Replicate" + "%%" + key + "%%" + value;
                        Socket socket;
                        OutputStream outputStream;
                        DataOutputStream dataOutputStream;
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(keyNode));
                        Log.i(TAG, "Sending Key Insert : " + keyNode);
                        outputStream = socket.getOutputStream();
                        dataOutputStream = new DataOutputStream(outputStream);
                        dataOutputStream.write(msgForward.getBytes());
                        socket.close();
                        //Socket socket;
                        //OutputStream outputStream;
                        //DataOutputStream dataOutputStream;
                        for (int i = 0; i < NEWREMOTE_PORTS.size(); i++) {
                            if (keyNode.equals(NEWREMOTE_PORTS.get(i))) {
                                if (i == 3) {


                                    String msgReplicate = myPort + "%%" + "Replicate" + "%%" + key + "%%" + value;
                                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            Integer.parseInt(NEWREMOTE_PORTS.get(i + 1)));
                                    Log.i(TAG, "Sending Replicate to : " + NEWREMOTE_PORTS.get(i + 1));
                                    outputStream = socket.getOutputStream();
                                    dataOutputStream = new DataOutputStream(outputStream);
                                    dataOutputStream.write(msgReplicate.getBytes());
                                    socket.close();

                                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            Integer.parseInt(NEWREMOTE_PORTS.get(0)));
                                    Log.i(TAG, "Sending Replicate to : " + NEWREMOTE_PORTS.get(0));
                                    outputStream = socket.getOutputStream();
                                    dataOutputStream = new DataOutputStream(outputStream);
                                    dataOutputStream.write(msgReplicate.getBytes());
                                    socket.close();
                                } else if (i == 4) {

                                    String msgReplicate = myPort + "%%" + "Replicate" + "%%" + key + "%%" + value;
                                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            Integer.parseInt(NEWREMOTE_PORTS.get(0)));
                                    Log.i(TAG, "Sending Replicate to : " + NEWREMOTE_PORTS.get(0));
                                    outputStream = socket.getOutputStream();
                                    dataOutputStream = new DataOutputStream(outputStream);
                                    dataOutputStream.write(msgReplicate.getBytes());
                                    socket.close();

                                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            Integer.parseInt(NEWREMOTE_PORTS.get(1)));
                                    Log.i(TAG, "Sending Replicate to : " + NEWREMOTE_PORTS.get(1));
                                    outputStream = socket.getOutputStream();
                                    dataOutputStream = new DataOutputStream(outputStream);
                                    dataOutputStream.write(msgReplicate.getBytes());
                                    socket.close();
                                } else {
                                    String msgReplicate = myPort + "%%" + "Replicate" + "%%" + key + "%%" + value;
                                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            Integer.parseInt(NEWREMOTE_PORTS.get(i + 1)));
                                    Log.i(TAG, "Sending Replicate to : " + NEWREMOTE_PORTS.get(i + 1));
                                    outputStream = socket.getOutputStream();
                                    dataOutputStream = new DataOutputStream(outputStream);
                                    dataOutputStream.write(msgReplicate.getBytes());
                                    socket.close();

                                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            Integer.parseInt(NEWREMOTE_PORTS.get(i + 2)));
                                    Log.i(TAG, "Sending Replicate to : " + NEWREMOTE_PORTS.get(i + 2));
                                    outputStream = socket.getOutputStream();
                                    dataOutputStream = new DataOutputStream(outputStream);
                                    dataOutputStream.write(msgReplicate.getBytes());
                                    socket.close();
                                }
                            }
                        }
                    }

                }
            } catch (Exception e) {
                e.getMessage();
            }

            return uri;

            //return null;
        }
    }



    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        try {
            //ArrayList<String> NEWREMOTE_PORTS = new ArrayList<String>();
            TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
            String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            myPort = String.valueOf((Integer.parseInt(portStr) * 2));
            port = String.valueOf((Integer.parseInt(portStr)));
            myPortHash = genHash(port);
            succPortHash = myPortHash;
            predPortHash = myPortHash;
            succPort = myPort;
            predPort = myPort;

            try {
                ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
                new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            } catch (IOException e) {
                Log.e(TAG, "Can't create a ServerSocket");
            }

            if (NEWREMOTE_PORTS.size()==0) {
                NEWREMOTE_PORTS.add(0, "11124");
                NEWREMOTE_PORTS.add(1, "11112");
                NEWREMOTE_PORTS.add(2, "11108");
                NEWREMOTE_PORTS.add(3, "11116");
                NEWREMOTE_PORTS.add(4, "11120");
            }




            for (int k = 0; k < NEWREMOTE_PORTS.size(); k++) {
                Log.i(TAG, "NEWREMOTE_PORTS: " + NEWREMOTE_PORTS.get(k));
                Log.i(TAG, "NEWREMOTE_PORTS size: " + NEWREMOTE_PORTS.size());
                if (myPort.equals(NEWREMOTE_PORTS.get(k))) {
                    {
                        if (k == 0) {
                            predPort = NEWREMOTE_PORTS.get(NEWREMOTE_PORTS.size() - 1);
                            succPort = NEWREMOTE_PORTS.get(1);
                        } else if (k == NEWREMOTE_PORTS.size() - 1) {
                            predPort = NEWREMOTE_PORTS.get(k - 1);
                            succPort = NEWREMOTE_PORTS.get(0);
                        } else {
                            predPort = NEWREMOTE_PORTS.get(k - 1);
                            succPort = NEWREMOTE_PORTS.get(k + 1);
                        }


                    }
                }
            }

            Log.i(TAG, "successor port: " + succPort);
            Log.i(TAG, "predecessor port: " + predPort);

         /*   aliveNodes.add(genHash("5554"));
            nodeLookup.put(genHash("5554"), "11108");
            if (!myPort.equals("11108")) {
                String req = "Node Join Request";
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, req);
*/

                    //synchronized (lock3){
                   //sendPing();
                        /*String req = "I am alive";
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, req);
                        //Log.i(TAG, "Sending Ping");
                    while (aliveCount>0 && aliveCount!=3)
                    {

                    }
                    if(aliveCount==3) {
                        //}
                        // synchronized (lock3) {
                        checkPing();
                        //alive = "";
                        Thread.sleep(1000);
                    }*/
                    //}
                    //synchronized (lock3) {
            String req = "RecoveryOp";
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, req);
                        Thread.sleep(4000);
            if (recoverCount<3)
            {
                Log.i(TAG, "Waiting for recover response ");
                Thread.sleep(2000);
            }
            if(recoverCount>2)
            {
                Log.i(TAG, "Enters Recovery Operation");
                Log.i(TAG, recoverResponse);
                if(recoverResponse.contains(";;")) {
                    String[] replies = recoverResponse.split(";;");
                    Log.i(TAG, "Replies size: " + replies.length);
                    if (replies.length > 0) {
                        for (int i = 0; i < replies.length; i++) {
                            if (replies[i].split("::").length > 1) {
                                Log.i(TAG, "Replies size: " + replies.length);
                                Log.i(TAG, "Replies i: " + replies[i].toString());
                                String[] row = replies[i].split("::");
                                String keyR = row[0];
                                String valueR = row[1];
                                Log.i(TAG, "Storecheck for: " + keyR);
                                String pos = storeCheck(keyR);
                                Log.i(TAG, "Printing pos: " + pos);
                                if (pos.contains(myPort)) {
                                    if (provider.get(keyR) == null) {
                                        Log.i(TAG, "(RECOVERY)NO existing value for key..inserting");
                                        provider.put(keyR, valueR);
                                    } else if (provider.get(keyR) != null) {
                                        Log.i(TAG, "(RECOVERY)Existing value found for key..removing first and inserting new value");
                                        provider.remove(keyR);
                                        provider.put(keyR, valueR);
                                    }
                                }


                            }
                        }
                    }
                }
                recoverCount = 0;
            }






        } catch (NoSuchAlgorithmException n) {
            n.printStackTrace();
        }
        catch (InterruptedException i)
        {
            i.getMessage();
        }



        return false;
    }


    @Override

    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        synchronized (lock2) {
            Log.i(TAG, "Query entered");
            matrixCursor = new MatrixCursor(columns);
            String[] splitQuery = selection.split("%%");
            String keyQuery = splitQuery[0];
            Log.i(TAG, "The matrix cursor size is: " + matrixCursor.getCount());
            Log.i(TAG, "The query received at query method is: " + keyQuery);
           /* try {
              //  Log.i(TAG, "The query key hash generated at query method is: " + genHash(keyQuery));
            } catch (NoSuchAlgorithmException n) {
                n.getMessage();
            }*/
            Log.i(TAG, "The splitquery length is: " + splitQuery.length);
/*
        if(splitQuery.length > 1 && splitQuery[2].equals("Found key"))
        {
            //matrixCursor= new MatrixCursor(columns);
            Log.i(TAG, "OMG, it found the key!: " + splitQuery[0]);
            String [] respfinal = {splitQuery[0], splitQuery[1]};
            matrixCursor.addRow(respfinal);
            Log.v(TAG, "added row to matrixcursor"+matrixCursor.getCount());
            //matrixCursor.moveToNext();
            //matrixCursor.close();
            return  matrixCursor;

        }*/
            // else {

            if (keyQuery.equals("@")) {

                if (provider.size() > 0) {
                    matrixCursor = new MatrixCursor(columns);
                    for (Map.Entry<String, String> row : provider.entrySet()) {
                        String[] response = {row.getKey(), row.getValue()};
                        matrixCursor.addRow(response);
                        Log.i(TAG, "The matrix cursor size from" + myPort + " is " + matrixCursor.getCount());

                    }
                }

            } else if (keyQuery.equals("*")) {
                Log.i(TAG, "Enters query case *");
                Log.i(TAG, "matching: " + succPort + predPort + myPort);
                if (myPort.equals(succPort) && myPort.equals(predPort)) {
                    //Set set = provider.entrySet();
                    matrixCursor = new MatrixCursor(columns);
                    for (Map.Entry<String, String> row : provider.entrySet()) {
                        String[] response = {row.getKey(), row.getValue()};
                        matrixCursor.addRow(response);

                    }
                    //cursor.addRow();
                } else {

                    try {
                        Log.i(TAG, "Enters query case ELSE in *");

                        String arg1 = "Asterisk Query";
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, arg1);

                        for (Map.Entry<String, String> row : provider.entrySet()) {
                            String[] response = {row.getKey(), row.getValue()};
                            matrixCursor.addRow(response);
                        }

                        if (queryResponse.equals("")) {
                            //while (queryResponse.equals("")) {
                            Thread.sleep(10000);
                            Log.i(TAG, "Sleeping 2");
                            //}
                        }

                        String[] responseParts = queryResponse.split(";;");
                        Log.i(TAG, "Query Response:" + responseParts.length);
                        if (responseParts.length > 0) {
                            HashMap<String, String> resp = new HashMap<String, String>();
                            for (int i = 0; i < responseParts.length; i++) {
                                String splitter = responseParts[i];
                                Log.i(TAG, "Query Response Received at:" + myPort + "is " + responseParts[i]);
                                String[] parts = (splitter.split("::"));
                                resp.put(parts[0], parts[1]);
                                for (Map.Entry<String, String> othermaps : resp.entrySet()) {
                                    matrixCursor.addRow(new String[]{othermaps.getKey(), othermaps.getValue()});

                                    // flag = true;
                                }
                            }
                        }


                    } catch (Exception e) {
                        e.getMessage();
                    }
                }
            } else {
                try {
                    Log.i(TAG, "Enters query case ELSE in query");
                    //String keyQueryHash = genHash(keyQuery);
                    if (splitQuery.length > 1 && splitQuery[1].equals("SingleQuery")) {
                        String value = provider.get(keyQuery);
                        String[] finalresp = {keyQuery, value};
                        //responseMap.add(keyQueryHash);
                        //provider.get(keyQueryHash);

                        matrixCursor.addRow(finalresp);
                    }
                    else if (provider.containsKey(keyQuery))
                    {
                        String value = provider.get(keyQuery);
                        String[] finalresp = {keyQuery, value};

                        matrixCursor.addRow(finalresp);
                    }
                    else {
                        String keyReplica = findReplicaPosition(keyQuery);
                        try {
                            if (keyReplica == null) {
                                while (keyReplica == null) {
                                    Thread.sleep(200);
                                }
                            }
                        } catch (Exception e) {
                            e.getMessage();
                        }
                        String[] keyNodes = keyReplica.split("::");
                        if (keyNodes.length > 1) {
                            String keyNode = keyNodes[0];
                            String keyNodeb = keyNodes[1];

                            Log.i(TAG, "Keynode is: " + keyNode + " myPort is: " + myPort);
                            if (keyNode.equals(myPort)) {
                                Log.i(TAG, "Enters first if case in ELSE");
                                //matrixCursor = new MatrixCursor(columns);
                                String value = provider.get(keyQuery);
                                String[] finalresp = {keyQuery, value};
                                //responseMap.add(keyQueryHash);
                                //provider.get(keyQueryHash);

                                matrixCursor.addRow(finalresp);
                                // Log.i(TAG, "Matrix Cursor return: " + matrixCursor.getString(matrixCursor.getColumnIndex("key")));
                                //matrixCursor.moveToNext();
                                //matrixCursor.close();
                                //return matrixCursor;
                            }
                            else if (keyNodeb.equals(myPort)) {
                                Log.i(TAG, "Enters first else if case in ELSE");
                                //matrixCursor = new MatrixCursor(columns);
                                String value = provider.get(keyQuery);
                                String[] finalresp = {keyQuery, value};
                                //responseMap.add(keyQueryHash);
                                //provider.get(keyQueryHash);

                                matrixCursor.addRow(finalresp);
                            }
                            else if (splitQuery.length == 1) {
                                //synchronized (lock2) {
                                    try {

                                        String req = "Single Query Forward";
                                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, req, keyQuery, keyNode, keyNodeb);
                                        Thread.sleep(200);
                                        if (queryMsg == null) {
                                            while (queryMsg == null) {
                                                Thread.sleep(500);
                                                Log.i(TAG, "Sleeping 1");
                                            }
                                        }


                                    } catch (Exception e) {
                                        e.getMessage();
                                    }

                                    String passMsg = queryMsg;
                                    queryMsg = null;
                                    matrixCursor = new MatrixCursor(columns);
                                    String[] response = passMsg.split("%%");
                                    // queryMsg = null;
                                    Log.i(TAG, "MESSAGE: " + response[0] + response[2]);
                                    String[] respfinal = {response[0], response[1]};
                                    matrixCursor.addRow(respfinal);
                                    Log.i(TAG, "Matrix Cursor rows return: " + matrixCursor.getCount());
                                    return matrixCursor;
                                    //Log.i(TAG, "Matrix Cursor rows return: " + matrixCursor.getCount());
                                }
                            }
                        }


                } catch (Exception e) {
                    e.getMessage();
                }

                Log.v("query", keyQuery);

                Log.i(TAG, "matrixcursor is returning from: " + myPort + "and its size is: " + matrixCursor.getCount());
            }
            return matrixCursor;
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }



    private String findPosition(String input) {
        Log.i(TAG, "Enters findPosition method");
        String positionNode = "";
        try {
            String hashedKey = genHash(input);
            //String value = values.getAsString("value");
            String portHash = "";
            // String nextPortHash = "";
            String prevPortHash = "";


            for (int k = 0; k < NEWREMOTE_PORTS.size(); k++) {
                if(k==0) {
                    Log.i(TAG, "Checking if position for :"+input+ "is -:" + NEWREMOTE_PORTS.get(k));
                    portHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k)) / 2 + "");
                    //hashedKey = genHash(input);
                    //nextPortHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k + 1)) / 2 + "");
                    prevPortHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(NEWREMOTE_PORTS.size()-1)) / 2 + "");

                    if (portHash.compareTo(prevPortHash) <= 0 && hashedKey.compareTo(prevPortHash) > 0 && hashedKey.compareTo(portHash) >= 0) {
                        try {
                            positionNode = NEWREMOTE_PORTS.get(k);
                            Log.i(TAG, "position node found for key :"+input+ "is -:" + NEWREMOTE_PORTS.get(k));


                        } catch (Exception e) {
                            e.getMessage();
                        }

                    } else if (portHash.compareTo(prevPortHash) <= 0 && hashedKey.compareTo(prevPortHash) < 0 && hashedKey.compareTo(portHash) <= 0) {
                        try {
                            positionNode = NEWREMOTE_PORTS.get(k);
                            Log.i(TAG, "position node found for key :"+input+ "is -:" + NEWREMOTE_PORTS.get(k));

                        } catch (Exception e) {
                            e.getMessage();
                        }

                    } else if (hashedKey.compareTo(prevPortHash) > 0 && hashedKey.compareTo(portHash) <= 0) {
                        try {
                            positionNode = NEWREMOTE_PORTS.get(k);
                            Log.i(TAG, "position node found for key :"+input+ "is -:" + NEWREMOTE_PORTS.get(k));
                        } catch (Exception e) {
                            e.getMessage();
                        }
                    }
                }
               /* else if( k == NEWREMOTE_PORTS.size()-1)
                {
                    portHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k)) / 2 + "");
                    hashedKey = genHash(input);
                    nextPortHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(0)) / 2 + "");
                    prevPortHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k-1)) / 2 + "");
                }*/
                else
                {
                    Log.i(TAG, "Checking if position for :"+input+ "is -:" + NEWREMOTE_PORTS.get(k));
                    portHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k)) / 2 + "");
                    hashedKey = genHash(input);
                    // nextPortHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k + 1)) / 2 + "");
                    prevPortHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k-1)) / 2 + "");

                    if (portHash.compareTo(prevPortHash) <= 0 && hashedKey.compareTo(prevPortHash) > 0 && hashedKey.compareTo(portHash) >= 0) {
                        try {
                            positionNode = NEWREMOTE_PORTS.get(k);
                            Log.i(TAG, "position node found for key :"+input+ "is -:" + NEWREMOTE_PORTS.get(k));


                        } catch (Exception e) {
                            e.getMessage();
                        }

                    } else if (portHash.compareTo(prevPortHash) <= 0 && hashedKey.compareTo(prevPortHash) < 0 && hashedKey.compareTo(portHash) <= 0) {
                        try {
                            positionNode = NEWREMOTE_PORTS.get(k);
                            Log.i(TAG, "position node found for key :"+input+ "is -:" + NEWREMOTE_PORTS.get(k));

                        } catch (Exception e) {
                            e.getMessage();
                        }

                    } else if (hashedKey.compareTo(prevPortHash) > 0 && hashedKey.compareTo(portHash) <= 0) {
                        try {
                            positionNode = NEWREMOTE_PORTS.get(k);
                            Log.i(TAG, "position node found for key :"+input+ "is -:" + NEWREMOTE_PORTS.get(k));
                        } catch (Exception e) {
                            e.getMessage();
                        }
                    }
                }


            }

        }
        catch (Exception e)
        {
            e.getMessage();
        }



        return positionNode;




    }

    private String findReplicaPosition(String input) {
        Log.i(TAG, "Enters findReplicaPosition method");
        String positionNode = "";
        try {
            String hashedKey = genHash(input);
            //String value = values.getAsString("value");
            String portHash = "";
            // String nextPortHash = "";
            String prevPortHash = "";


            for (int k = 0; k < NEWREMOTE_PORTS.size(); k++) {

               /* else if( k == NEWREMOTE_PORTS.size()-1)
                {
                    portHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k)) / 2 + "");
                    hashedKey = genHash(input);
                    nextPortHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(0)) / 2 + "");
                    prevPortHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k-1)) / 2 + "");
                }*/
                if(k==0)
                {
                    Log.i(TAG, "Checking if position for :"+input+ "is -:" + NEWREMOTE_PORTS.get(k));
                    portHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k)) / 2 + "");
                    hashedKey = genHash(input);
                    // nextPortHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k + 1)) / 2 + "");
                    prevPortHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(NEWREMOTE_PORTS.size()-1)) / 2 + "");

                    if (portHash.compareTo(prevPortHash) <= 0 && hashedKey.compareTo(prevPortHash) > 0 && hashedKey.compareTo(portHash) >= 0) {
                        try {
                            positionNode = NEWREMOTE_PORTS.get(k+2)+"::"+NEWREMOTE_PORTS.get(k+1);
                            Log.i(TAG, "replica position node found for query of key :"+input+ "is -:" + NEWREMOTE_PORTS.get(k+2));


                        } catch (Exception e) {
                            e.getMessage();
                        }

                    } else if (portHash.compareTo(prevPortHash) <= 0 && hashedKey.compareTo(prevPortHash) < 0 && hashedKey.compareTo(portHash) <= 0) {
                        try {
                            positionNode = NEWREMOTE_PORTS.get(k+2)+"::"+NEWREMOTE_PORTS.get(k+1);
                            Log.i(TAG, "replica position node found for query of key :"+input+ "is -:" + NEWREMOTE_PORTS.get(k+2));

                        } catch (Exception e) {
                            e.getMessage();
                        }

                    } else if (hashedKey.compareTo(prevPortHash) > 0 && hashedKey.compareTo(portHash) <= 0) {
                        try {
                            positionNode = NEWREMOTE_PORTS.get(k+2)+"::"+NEWREMOTE_PORTS.get(k+1);
                            Log.i(TAG, "replica position node found for query of key :"+input+ "is -:" + NEWREMOTE_PORTS.get(k+2));
                        } catch (Exception e) {
                            e.getMessage();
                        }
                    }
                }
                else if(k==4)
                {
                    Log.i(TAG, "Checking if position for :"+input+ "is -:" + NEWREMOTE_PORTS.get(k));
                    portHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k)) / 2 + "");
                    hashedKey = genHash(input);
                    // nextPortHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k + 1)) / 2 + "");
                    prevPortHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k-1)) / 2 + "");

                    if (portHash.compareTo(prevPortHash) <= 0 && hashedKey.compareTo(prevPortHash) > 0 && hashedKey.compareTo(portHash) >= 0) {
                        try {
                            positionNode = NEWREMOTE_PORTS.get(1)+"::"+NEWREMOTE_PORTS.get(0);

                            Log.i(TAG, "replica position node found for query of key :"+input+ "is -:" + NEWREMOTE_PORTS.get(1));


                        } catch (Exception e) {
                            e.getMessage();
                        }

                    } else if (portHash.compareTo(prevPortHash) <= 0 && hashedKey.compareTo(prevPortHash) < 0 && hashedKey.compareTo(portHash) <= 0) {
                        try {
                            positionNode = NEWREMOTE_PORTS.get(1)+"::"+NEWREMOTE_PORTS.get(0);
                            Log.i(TAG, "replica position node found for query of key :"+input+ "is -:" + NEWREMOTE_PORTS.get(1));

                        } catch (Exception e) {
                            e.getMessage();
                        }

                    } else if (hashedKey.compareTo(prevPortHash) > 0 && hashedKey.compareTo(portHash) <= 0) {
                        try {
                            positionNode = NEWREMOTE_PORTS.get(1)+"::"+NEWREMOTE_PORTS.get(0);
                            Log.i(TAG, "replica position node found for query of key :"+input+ "is -:" + NEWREMOTE_PORTS.get(1));
                        } catch (Exception e) {
                            e.getMessage();
                        }
                    }
                }
                else if(k==3)
                {
                    Log.i(TAG, "Checking if position for :"+input+ "is -:" + NEWREMOTE_PORTS.get(k));
                    portHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k)) / 2 + "");
                    //hashedKey = genHash(input);
                    //nextPortHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k + 1)) / 2 + "");
                    prevPortHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k-1)) / 2 + "");

                    if (portHash.compareTo(prevPortHash) <= 0 && hashedKey.compareTo(prevPortHash) > 0 && hashedKey.compareTo(portHash) >= 0) {
                        try {
                            positionNode = NEWREMOTE_PORTS.get(0)+"::"+NEWREMOTE_PORTS.get(k+1);

                            Log.i(TAG, "replica position node found for query of key :"+input+ "is -:" + NEWREMOTE_PORTS.get(0));


                        } catch (Exception e) {
                            e.getMessage();
                        }

                    } else if (portHash.compareTo(prevPortHash) <= 0 && hashedKey.compareTo(prevPortHash) < 0 && hashedKey.compareTo(portHash) <= 0) {
                        try {
                            positionNode = NEWREMOTE_PORTS.get(0)+"::"+NEWREMOTE_PORTS.get(k+1);
                            Log.i(TAG, "replica position node found for query of key :"+input+ "is -:" + NEWREMOTE_PORTS.get(0));

                        } catch (Exception e) {
                            e.getMessage();
                        }

                    } else if (hashedKey.compareTo(prevPortHash) > 0 && hashedKey.compareTo(portHash) <= 0) {
                        try {
                            positionNode = NEWREMOTE_PORTS.get(0)+"::"+NEWREMOTE_PORTS.get(k+1);
                            Log.i(TAG, "replica position node found for query of key :"+input+ "is -:" + NEWREMOTE_PORTS.get(0));
                        } catch (Exception e) {
                            e.getMessage();
                        }
                    }
                }
                else
                {
                    Log.i(TAG, "Checking if position for :"+input+ "is -:" + NEWREMOTE_PORTS.get(k));
                    portHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k)) / 2 + "");
                    //hashedKey = genHash(input);
                    //nextPortHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k + 1)) / 2 + "");
                    prevPortHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k-1)) / 2 + "");

                    if (portHash.compareTo(prevPortHash) <= 0 && hashedKey.compareTo(prevPortHash) > 0 && hashedKey.compareTo(portHash) >= 0) {
                        try {

                            positionNode = NEWREMOTE_PORTS.get(k+2)+"::"+NEWREMOTE_PORTS.get(k+1);
                            Log.i(TAG, "replica position node found for query of key :"+input+ "is -:" + NEWREMOTE_PORTS.get(k+2));


                        } catch (Exception e) {
                            e.getMessage();
                        }

                    } else if (portHash.compareTo(prevPortHash) <= 0 && hashedKey.compareTo(prevPortHash) < 0 && hashedKey.compareTo(portHash) <= 0) {
                        try {
                            positionNode = NEWREMOTE_PORTS.get(k+2)+"::"+NEWREMOTE_PORTS.get(k+1);
                            Log.i(TAG, "replica position node found for query of key :"+input+ "is -:" + NEWREMOTE_PORTS.get(k+2));

                        } catch (Exception e) {
                            e.getMessage();
                        }

                    } else if (hashedKey.compareTo(prevPortHash) > 0 && hashedKey.compareTo(portHash) <= 0) {
                        try {
                            positionNode = NEWREMOTE_PORTS.get(k+2)+"::"+NEWREMOTE_PORTS.get(k+1);
                            Log.i(TAG, "replica position node found for query of key :"+input+ "is -:" + NEWREMOTE_PORTS.get(k+2));
                        } catch (Exception e) {
                            e.getMessage();
                        }
                    }
                }

            }

        }
        catch (Exception e)
        {
            e.getMessage();
        }



        return positionNode;




    }
    private void sendPing() {
        Log.i(TAG, "Finding if first run or recovery");
        try {
            //String portNo = input;
            OutputStream outputStream;
            DataOutputStream dataOutputStream;
            String msgPing = myPort + "%%" + "I am alive";
            for (int i = 0; i < NEWREMOTE_PORTS.size(); i++) {
                if (!myPort.equals(NEWREMOTE_PORTS.get(i))) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(NEWREMOTE_PORTS.get(i)));
                    Log.i(TAG, "Sending Ping to : " + NEWREMOTE_PORTS.get(i));
                    outputStream = socket.getOutputStream();
                    dataOutputStream = new DataOutputStream(outputStream);
                    dataOutputStream.write(msgPing.getBytes());
                    socket.close();
                }
            }
        } catch (Exception e) {
            e.getMessage();

        }
    }

    private void checkPing() {
        Log.i(TAG, "Checking all pings");
        try {

            for(int i = 0; i<NEWREMOTE_PORTS.size(); i++) {
                if (!myPort.equals(NEWREMOTE_PORTS.get(i))) {
                    if (aliveCount==3 && alive.contains(NEWREMOTE_PORTS.get(i))) {
                        Log.i(TAG, "alive string: " + alive);
                        Log.i(TAG, "The node is alive: " + NEWREMOTE_PORTS.get(i));
                        aliveCount = 0;
                    }
                    else if(!alive.contains(NEWREMOTE_PORTS.get(i)) && aliveCount == 3)
                    {
                        Log.i(TAG, "alive string: " + alive);
                        Log.i(TAG, "The node has failed: " + NEWREMOTE_PORTS.get(i));
                        failedNode = NEWREMOTE_PORTS.get(i);
                        aliveCount = 0;
                    }

                }
            }
            alive = "";
        } catch (Exception e) {
            e.getMessage();

        }
    }
    private void recoveryAsk() {
        Log.i(TAG, "Finding if first run or recovery");
        try {
            OutputStream outputStream;
            DataOutputStream dataOutputStream;
            String msgRecoveryFind = myPort+"Am I Recovering";
            //for (int i = 0; i < 2; i++) {
                //if (!myPort.equals(NEWREMOTE_PORTS.get(i))) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(succPort));
                    Log.i(TAG, "Sending Recovery Checker to : " + succPort);
                    outputStream = socket.getOutputStream();
                    dataOutputStream = new DataOutputStream(outputStream);
                    dataOutputStream.write(msgRecoveryFind.getBytes());
                    socket.close();


        }
        catch(Exception e){
            e.getMessage();

        }

        //return null;
    }



    private void recoveryOp() {
        Log.i(TAG, "Recovery Started");
        try {
            //OutputStream outputStream;
            //DataOutputStream dataOutputStream;
            String msgRec = myPort+"Send Data";
            //for (int i = 0; i < 2; i++) {
            //if (!myPort.equals(NEWREMOTE_PORTS.get(i))) {
            for (int i = NEWREMOTE_PORTS.size() - 1; i >= 0; i--) {
                if (succPort.equals(NEWREMOTE_PORTS.get(i))) {

                   // Log.i(TAG, "NEWREMOTE_PORTS size is" + NEWREMOTE_PORTS.size());

                    if (i == NEWREMOTE_PORTS.size() - 1) {
                        Log.i(TAG, "Value of i is: " + i);
                        Socket socket;
                        OutputStream outputStream;
                        DataOutputStream dataOutputStream;
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(succPort));
                        Log.i(TAG, "Requesting recover data from : " + succPort);
                        outputStream = socket.getOutputStream();
                        dataOutputStream = new DataOutputStream(outputStream);
                        dataOutputStream.write(msgRec.getBytes());
                        socket.close();

                        Socket newsocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(NEWREMOTE_PORTS.get(0)));
                        Log.i(TAG, "Requesting recover data from : " + NEWREMOTE_PORTS.get(0));
                        outputStream = newsocket.getOutputStream();
                        dataOutputStream = new DataOutputStream(outputStream);
                        dataOutputStream.write(msgRec.getBytes());
                        newsocket.close();

                    } else {
                        Socket socket;
                        OutputStream outputStream;
                        DataOutputStream dataOutputStream;
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(succPort));
                        Log.i(TAG, "Requesting recover data from : " + succPort);
                        outputStream = socket.getOutputStream();
                        dataOutputStream = new DataOutputStream(outputStream);
                        dataOutputStream.write(msgRec.getBytes());
                        socket.close();

                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(NEWREMOTE_PORTS.get(i + 1)));
                        Log.i(TAG, "Requesting recover data from : " + NEWREMOTE_PORTS.get(i+1));
                        outputStream = socket.getOutputStream();
                        dataOutputStream = new DataOutputStream(outputStream);
                        dataOutputStream.write(msgRec.getBytes());
                        socket.close();
                    }
                }
            }


        }
        catch(Exception e){
            e.getMessage();

        }

        //return null;
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected  Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
            while (true) {
                try {
                    Socket socket = serverSocket.accept();

                    BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String msg = br.readLine();
                    socket.close();
                    //publishProgress(new String(incomingMessage));
                    String[] msgParts = msg.split("%%");
                    String msgFromPort = msgParts[0];
                    String msgText = msgParts[1];

                    if (msgText.contains("Node Join Request")) {
                        try {
                            //ArrayList<String> aliveNodes = new ArrayList<String>();
                            String msgFromPortHash = genHash(Integer.parseInt(msgFromPort) / 2 + "");
                            //REMOTE_PORTS.add(msgFromPort);
                            aliveNodes.add(msgFromPortHash);
                            Log.i(TAG, msgFromPortHash);
                            Log.i(TAG, msgFromPort);

                            nodeLookup.put(msgFromPortHash, msgFromPort);

                            Collections.sort(aliveNodes);

                            REMOTE_PORTS.clear();
                            for (int i = 0; i < aliveNodes.size(); i++) {
                                String value = nodeLookup.get(aliveNodes.get(i));
                                REMOTE_PORTS.add(value);
                            }
                            for (int i = 0; i < REMOTE_PORTS.size(); i++) {
                                Log.i(TAG, "Remote Ports:" + REMOTE_PORTS.get(i));
                            }
                            Log.i(TAG, REMOTE_PORTS.size() + "");


                            String sendPorts = "";
                            String sendPortHash = "";
                            for (int i = 0; i < REMOTE_PORTS.size(); i++) {
                                sendPorts = sendPorts + REMOTE_PORTS.get(i) + "::";
                            }

                            for (int i = 0; i < aliveNodes.size(); i++) {
                                sendPortHash = sendPortHash + aliveNodes.get(i) + "::";

                            }


                            OutputStream outputStream;
                            DataOutputStream dataOutputStream;

                            for (int i = 0; i < REMOTE_PORTS.size(); i++) {
                                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(REMOTE_PORTS.get(i)));
                                String msgList = sendPorts + "%%" + "Update Nodes" + "%%" + sendPortHash + "%%" + REMOTE_PORTS.size();

                                outputStream = socket.getOutputStream();
                                dataOutputStream = new DataOutputStream(outputStream);
                                dataOutputStream.write(msgList.getBytes());
                                socket.close();
                            }


                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    } else if (msgText.contains("Update Nodes")) {
                        try {
                            //ArrayList<String> aliveNodes = new ArrayList<String>();
                            //String msgFromPortHash = genHash(msgFromPort);
                            //REMOTE_PORTS.add(msgFromPort);
                            //aliveNodes.add(msgFromPortHash);
                            //wait(5);
                            //Collections.sort(aliveNodes);
                            TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
                            String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
                            final String myPort = String.valueOf((Integer.parseInt(portStr)));


                            String portHashes = msgParts[2];
                            Log.i(TAG, "port hashes:" + portHashes);
                            String[] portHash = portHashes.split("::");
                            for (int i = 0; i < portHash.length; i++) {
                                Log.i(TAG, "port hash:" + portHash[i]);
                            }
                            String[] ports = msgParts[0].split("::");
                            for (int i = 0; i < ports.length; i++) {
                                Log.i(TAG, "ports:" + ports[i]);
                            }

                            aliveNodesList.clear();
                            updateLookup.clear();
                            for (int i = 0; i < portHash.length; i++) {
                                aliveNodesList.add(portHash[i]);
                                updateLookup.put(portHash[i], ports[i]);
                            }

                            Collections.sort(aliveNodesList);

                            //if(aliveNodesList.size()==Integer.parseInt(size)) {
                            NEWREMOTE_PORTS.clear();
                            for (int i = 0; i < aliveNodesList.size(); i++) {
                                String value = updateLookup.get(aliveNodesList.get(i));
                                NEWREMOTE_PORTS.add(value);
                            }
                            for (int i = 0; i < NEWREMOTE_PORTS.size(); i++) {
                                Log.i(TAG, "Remote Ports Updated:" + NEWREMOTE_PORTS.get(i));
                            }
                            Log.i(TAG, NEWREMOTE_PORTS.size() + "");


                            for (int i = 0; i < aliveNodesList.size(); i++) {
                                Log.i(TAG, "Alive Nodes:" + aliveNodesList.get(i));
                            }
                            Log.i(TAG, aliveNodesList.size() + "");


                            for (int k = 0; k < NEWREMOTE_PORTS.size(); k++) {
                                if (aliveNodesList.get(k).equals(myPortHash)) {
                                    if (k == 0) {
                                        predPort = NEWREMOTE_PORTS.get(NEWREMOTE_PORTS.size() - 1);
                                        succPort = NEWREMOTE_PORTS.get(1);
                                    } else if (k == NEWREMOTE_PORTS.size() - 1) {
                                        predPort = NEWREMOTE_PORTS.get(k - 1);
                                        succPort = NEWREMOTE_PORTS.get(0);
                                    } else {
                                        predPort = NEWREMOTE_PORTS.get(k - 1);
                                        succPort = NEWREMOTE_PORTS.get(k + 1);
                                    }


                                }
                            }


                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    } else if (msgText.contains("Key Insert")) {
                        try {
                            Log.i(TAG, "Received Key Insert Query at: " + myPort);
                            String keyForward = msgParts[2];
                            String valueForward = msgParts[3];
                            ContentValues cv = new ContentValues();
                            cv.put("key", keyForward);
                            cv.put("value", valueForward);
                            Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                            insert(uri, cv);
                        } catch (Exception e) {
                            e.getMessage();
                        }
                    }

                    else if (msgText.contains("Replicate")) {
                        try {

                            Log.i(TAG, "Received Replicate at :"+ myPort);
                            String keyForward = msgParts[2];
                            String valueForward = msgParts[3];

                            /*ContentValues cv = new ContentValues();
                            cv.put("key", keyForward);
                            cv.put("value", valueForward);
                            Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                            insert(uri, cv);*/
                            if (provider.get(keyForward)==null) {
                                Log.i(TAG, "NO existing value for key..inserting");
                                provider.put(keyForward, valueForward);
                            }
                            else if(provider.get(keyForward)!=null)
                            {
                                Log.i(TAG, "Existing value found for key..removing first and inserting new value");
                                provider.remove(keyForward);
                                provider.put(keyForward,valueForward);
                            }

                           // provider.put(keyForward, valueForward);
                            Log.i(TAG, "Replication ho gaya in Server Task");
                        } catch (Exception e) {
                            e.getMessage();
                        }
                    }
                    else if (msgText.contains("Asterisk Delete")) {
                        try {
                            String keyDelete = msgParts[2];
                            Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                            int d = delete(uri, keyDelete, null);
                        } catch (Exception e) {
                            e.getMessage();
                        }
                    } else if (msgText.contains("KeyDelete")) {
                        try {
                            String recdkey = msgParts[2];
                            Log.i(TAG, "Delete Key received at: " +myPort + " is " +recdkey);
                            if(!recdkey.equals(keyDelete)) {
                                keyDelete = msgParts[2];
                                Log.i(TAG, "Forwarding Delete Key: " +keyDelete + " to " +myPort);
                                Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                                int d = delete(uri, keyDelete, null);
                            }
                        } catch (Exception e) {
                            e.getMessage();
                        }
                    } else if (msgText.contains("Asterisk Query")) {
                        try {
                            String keyQuery = "@";
                            Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                            Log.i(TAG, "Received Asterisk Query at: " + myPort);
                            Log.i(TAG, "Sending out " + keyQuery + " to all nodes via query");
                            Cursor newCursor = query(uri, null, keyQuery, null, null);
                            //String cursorResponse = "";
                            if (newCursor != null && newCursor.getCount() != 0) {
                                newCursor.moveToFirst();
                                String key = newCursor.getString(newCursor.getColumnIndex("key"));
                                String value = newCursor.getString(newCursor.getColumnIndex("value"));
                                String cursorResponse = key + "::" + value + ";;";
                                while (newCursor.moveToNext()) {
                                    String keynext = newCursor.getString(newCursor.getColumnIndex("key"));
                                    String valuenext = newCursor.getString(newCursor.getColumnIndex("value"));
                                    cursorResponse = cursorResponse + keynext + "::" + valuenext + ";;";
                                }

                                OutputStream outputStream;
                                DataOutputStream dataOutputStream;
                                //for (int i = REMOTE_PORTS.size() - 1; i >= 0; i--) {
                                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(msgFromPort));
                                String msgCompile = myPort + "%%" + "Compile" + "%%" + cursorResponse;
                                Log.i(TAG, "Sending back cursorResponse from :" + myPort + " to " + msgFromPort);
                                Log.i(TAG, "Cursor Response: " + cursorResponse);
                                outputStream = socket.getOutputStream();
                                dataOutputStream = new DataOutputStream(outputStream);
                                dataOutputStream.write(msgCompile.getBytes());
                                newCursor.close();
                                socket.close();
                            }
                        } catch (Exception e) {
                            e.getMessage();
                        }


                    } else if (msgText.contains("Compile")) {
                        //String queryResponse = "";


                        String appendResponse = msgParts[2];
                        Log.i(TAG, "The Append Response is :" + appendResponse);
                        queryResponse = queryResponse + appendResponse;
                        responseCount = responseCount + 1;

                        Log.i(TAG, "Received Compile Asterisk Query at " + myPort + "from " + msgFromPort + "and response count = " + responseCount);
                        if (queryResponse != null) {
                            String[] size = queryResponse.split("%%");

                            Log.i(TAG, "Query Response: " + size.length + "---" + queryResponse);
                        }


                    } else if (msgText.contains("Single Query Forward")) {
                        //Log.i(TAG, "Received Single Query Forward Message at: "+myPort);
                        String queryKey = msgParts[2];
                        Log.i(TAG, "Query key received: " + queryKey);
                        String originPort = msgParts[3];
                        Log.i(TAG, "Received Single Query Forward Message at: " + myPort + "from: " + originPort);
                        Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                        String queryForward = queryKey + "%%" + "SingleQuery" + "%%" + originPort;
                        Log.i(TAG, "queryFoward string: " + queryForward);
                        /*Cursor cursor = query(uri, null, queryForward, null, null);
                        if (cursor != null && cursor.getCount() != 0) {
                            cursor.moveToPosition(cursor.getCount() - 1);*/

                            String key = queryKey;
                            String value = provider.get(key);
                        Log.i(TAG, "Key Query Found at: " + myPort);
                        //String[] finalresp = {key, value};
                            String singleResponseMsg = myPort + "%%" + "Single Query Response" + "%%" + key + "::" + value;
                            //cursor.close();
                            Log.i(TAG, "The key found is : " + key);
                            try {
                                //Socket socket;
                                OutputStream outputStream;
                                DataOutputStream dataOutputStream;
                                //for (int i = REMOTE_PORT.length - 1; i >= 0; i--) {
                                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(originPort));
                                Log.i(TAG, "Sending back key : " + key + "to : " +originPort);
                                outputStream = socket.getOutputStream();
                                dataOutputStream = new DataOutputStream(outputStream);
                                dataOutputStream.write(singleResponseMsg.getBytes());
                                socket.close();

                            } catch (UnknownHostException e) {
                                Log.e(TAG, "Node Join UnknownHostException");
                                e.printStackTrace();
                            } catch (IOException e) {




                        }


                    }
                    else if (msgText.contains("I am alive")) {
                        alive = alive + "::" + msgFromPort;
                        aliveCount++;
                        Log.i(TAG, "writing to alive string: " + msgFromPort);
                        Log.i(TAG, "Ping received from : " + msgFromPort);


                    }
                    else if (msgText.contains("Am I recovering")) {

                        try {
                            //Socket socket;
                            OutputStream outputStream;
                            DataOutputStream dataOutputStream;
                            String msgReply = myPort +"%%"+ "Recovery Reply"+"%%" + failedNode;
                            //for (int i = REMOTE_PORT.length - 1; i >= 0; i--) {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(msgFromPort));
                            Log.i(TAG, "Reply to recoveryAsk from:" + myPort +" to: " +msgFromPort);
                            outputStream = socket.getOutputStream();
                            dataOutputStream = new DataOutputStream(outputStream);
                            dataOutputStream.write(msgReply.getBytes());
                            socket.close();
                            failedNode = "none";

                        } catch (UnknownHostException e) {
                            Log.e(TAG, "Node Join UnknownHostException");
                            e.printStackTrace();
                        }

                    }
                    else if (msgText.contains("Recovery Reply")) {

                        try {
                            //Socket socket;
                            String reply = msgParts[2];
                            if(reply.equals(myPort))
                            {
                                Log.i(TAG, "Initiate recovery at " + myPort);
                                recoveryOp();
                            }
                            else{
                                Log.e(TAG, "All good here at :" +myPort);
                            }

                        } catch (Exception e) {
                            Log.e(TAG, "Exception");
                            e.printStackTrace();
                        }

                    }
                    else if (msgText.contains("Send Data")) {

                        try {
                            String response = "";
                            //Socket socket;
                            if(provider.size()>0) {
                                for (Map.Entry<String, String> row : provider.entrySet()) {
                                    response = response +";;"+ row.getKey() + "::" + row.getValue();
                                }

                                OutputStream outputStream;
                                DataOutputStream dataOutputStream;
                                //for (int i = REMOTE_PORTS.size() - 1; i >= 0; i--) {
                                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(msgFromPort));
                                String msgBack = myPort + "%%" + "RecoverData" + "%%" + response;
                                Log.i(TAG, "Sending back RecoveryResponse from :" + myPort + " to " + msgFromPort);
                                //Log.i(TAG, "Cursor Response: " + cursorResponse);
                                outputStream = socket.getOutputStream();
                                dataOutputStream = new DataOutputStream(outputStream);
                                dataOutputStream.write(msgBack.getBytes());
                                //newCursor.close();
                                socket.close();
                            }


                        } catch (Exception e) {
                            Log.e(TAG, "Exception");
                            e.printStackTrace();
                        }

                    }
                    else if (msgText.contains("RecoverData")) {

                        try {

                            //Socket socket;
                            recoverResponse = recoverResponse + msgParts[2];
                            Log.i(TAG, "Recover Response Received at: " + myPort + "from: " + msgFromPort);

                            recoverCount++;

                            Log.i(TAG, "Recover count at: " + myPort + "is: " + recoverCount);
                            /*if(recoverCount>1)
                            {
                                Log.i(TAG, "Enters Recovery Operation");
                                Log.i(TAG, recoverResponse);
                                if(recoverResponse.contains("#"))
                                {
                                String[] replies = recoverResponse.split("#");
                                Log.i(TAG, "Replies size: " + replies.length);
                                for (int i=0; i<replies.length; i++)
                                {
                                    String[] row = replies[i].split("::");
                                    String keyR = row[0];
                                    String valueR = row[1];
                                    Log.i(TAG, "Storecheck for: " +keyR);
                                    String pos = storeCheck(keyR);
                                    Log.i(TAG, "Printing pos: "+pos);
                                    if(pos.contains(myPort))
                                    {
                                        if (provider.get(keyR)==null) {
                                            Log.i(TAG, "(RECOVERY)NO existing value for key..inserting");
                                            provider.put(keyR, valueR);
                                        }
                                        else if(provider.get(keyR)!=null)
                                        {
                                            Log.i(TAG, "(RECOVERY)Existing value found for key..removing first and inserting new value");
                                            provider.remove(keyR);
                                            provider.put(keyR, valueR);
                                        }
                                    }


                                }
                                }
                                recoverCount = 0;
                            }*/

                        } catch (Exception e) {
                            e.getMessage();
                        }
                    }
                    else if (msgText.contains("Single Query Response")) {
                        Log.i(TAG, "Single Query Response Received at: " + myPort + "from: " + msgFromPort);
                        String singleResponse = msgParts[2];
                        String[] queryResp = singleResponse.split("::");
                        queryMsg = queryResp[0] + "%%" + queryResp[1] + "%%" + "Found key";

                        Log.i(TAG, "Setting queryMsg: " + queryMsg + " at: " + myPort);


                    }
                } catch (Exception e) {
                    e.getMessage();
                }
            }
        }


        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }
    }

    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author stevko
     */
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            if(msgs.length > 1 && msgs[1].contains("Node Join Request"))
            {

                String msgToSend = msgs[0] + "%%" + "Node Join Request";
                try {
                    Socket socket;
                    OutputStream outputStream;
                    DataOutputStream dataOutputStream;

                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            11108);
                    outputStream = socket.getOutputStream();
                    dataOutputStream = new DataOutputStream(outputStream);
                    dataOutputStream.write(msgToSend.getBytes());
                    socket.close();

                } catch (UnknownHostException e) {
                    Log.e(TAG, "Node Join UnknownHostException");
                    e.printStackTrace();
                } catch (IOException e) {

                    Log.e(TAG, "Node Join socket IOException");
                    e.printStackTrace();
                }


            }
            else if (msgs.length > 1 && msgs[1].contains("Asterisk Query")) {
                String msgQuery = msgs[0] + "%%" + "Asterisk Query" + "%%" + "Local";

                try {
                    Socket socket;
                    OutputStream outputStream;
                    DataOutputStream dataOutputStream;
                    for (int i = NEWREMOTE_PORTS.size() - 1; i >= 0; i--) {
                        if (!myPort.equals(NEWREMOTE_PORTS.get(i))) {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(NEWREMOTE_PORTS.get(i)));
                            Log.i(TAG, "Sending Asterisk Query Msg to: " + NEWREMOTE_PORTS.get(i));
                            outputStream = socket.getOutputStream();
                            dataOutputStream = new DataOutputStream(outputStream);
                            dataOutputStream.write(msgQuery.getBytes());
                            socket.close();

                        }
                    }
                } catch (UnknownHostException e) {
                    Log.e(TAG, "Node Join UnknownHostException");
                    e.printStackTrace();
                } catch (IOException e) {

                    Log.e(TAG, "Node Join socket IOException");
                    e.printStackTrace();
                }
            }


                /*String originNode = msgs[0];
                int cpSize = provider.size();
                if(cpSize>1)
                {
                    try {
                        Socket socket;
                        String msgForwardQuery = myPort + "%%" + "Single Query Forward" + "%%" + keyQuery + "%%" + originPort;


                        OutputStream outputStream;
                        DataOutputStream dataOutputStream;
                    *//*for (int i = 0; i < NEWREMOTE_PORTS.size(); i++) {
                        if (destNode.equals(NEWREMOTE_PORTS.get(i)) && i == 4) {*//*
                        Log.i(TAG, "Sending Recovery Assurance to : " + originNode);
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(originNode));
                        //String msgCompile = myPortHash + "%%" + "Compile Asterisk Query" + "%%" + cursorResponse;
                        outputStream = socket.getOutputStream();
                        dataOutputStream = new DataOutputStream(outputStream);
                        dataOutputStream.write(msgForwardQuery.getBytes());
                        socket.close();
                    }
                    catch(Exception e)
                    {

                    }*/


            else if(msgs.length>1 && msgs[1].contains("Single Query Forward"))  {
                try {


                    Socket socket;
                    String originPort = msgs[0];
                    String keyQuery = msgs[2];
                    String destNode = msgs[3];
                    String destNodeb = msgs[4];
                    String msgForwardQuery = myPort + "%%" + "Single Query Forward" + "%%" + keyQuery + "%%" + originPort;


                    OutputStream outputStream;
                    DataOutputStream dataOutputStream;
                    /*for (int i = 0; i < NEWREMOTE_PORTS.size(); i++) {
                        if (destNode.equals(NEWREMOTE_PORTS.get(i)) && i == 4) {*/
                            Log.i(TAG, "Sending Single Query Forward to replica 2: " + destNode);
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(destNode));
                            //String msgCompile = myPortHash + "%%" + "Compile Asterisk Query" + "%%" + cursorResponse;
                            outputStream = socket.getOutputStream();
                            dataOutputStream = new DataOutputStream(outputStream);
                            dataOutputStream.write(msgForwardQuery.getBytes());
                            socket.close();
                    Log.i(TAG, "Sending Single Query Forward to replica 1: " + destNodeb);
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(destNodeb));
                    //String msgCompile = myPortHash + "%%" + "Compile Asterisk Query" + "%%" + cursorResponse;
                    outputStream = socket.getOutputStream();
                    dataOutputStream = new DataOutputStream(outputStream);
                    dataOutputStream.write(msgForwardQuery.getBytes());
                    socket.close();
                        /*} else if (destNode.equals(NEWREMOTE_PORTS.get(i)) && i == 3) {
                            Log.i(TAG, "Sending Single Query Forward for: " + destNode + " to replica 2: " + NEWREMOTE_PORTS.get(0));
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(NEWREMOTE_PORTS.get(0)));
                            //String msgCompile = myPortHash + "%%" + "Compile Asterisk Query" + "%%" + cursorResponse;
                            outputStream = socket.getOutputStream();
                            dataOutputStream = new DataOutputStream(outputStream);
                            dataOutputStream.write(msgForwardQuery.getBytes());
                            socket.close();
                        } else if (destNode.equals(NEWREMOTE_PORTS.get(i)) && (i == 0 || i==1 || i==2)) {
                            Log.i(TAG, "Sending Single Query Forward for: " + destNode + " to replica 2: " + NEWREMOTE_PORTS.get(i+2));
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(NEWREMOTE_PORTS.get(i+2)));
                            //String msgCompile = myPortHash + "%%" + "Compile Asterisk Query" + "%%" + cursorResponse;
                            outputStream = socket.getOutputStream();
                            dataOutputStream = new DataOutputStream(outputStream);
                            dataOutputStream.write(msgForwardQuery.getBytes());
                            socket.close();
                        }*/
                   // }
                }
                    catch(Exception e)
                    {
                        e.getMessage();
                    }
                    }
            else if(msgs.length>1 && msgs[1].contains("RecoveryOp"))  {

                Log.i(TAG, "Recovering data if any");
                try {
                    //OutputStream outputStream;
                    //DataOutputStream dataOutputStream;
                    String msgRec = myPort+ "%%"+"Send Data";
                    //for (int i = 0; i < 2; i++) {
                    //if (!myPort.equals(NEWREMOTE_PORTS.get(i))) {
                    for (int i = NEWREMOTE_PORTS.size() - 1; i >= 0; i--) {
                        if (succPort.equals(NEWREMOTE_PORTS.get(i))) {

                            // Log.i(TAG, "NEWREMOTE_PORTS size is" + NEWREMOTE_PORTS.size());

                            if (i == NEWREMOTE_PORTS.size() - 1) {
                                Log.i(TAG, "Value of i is: " + i);
                                Socket socket;
                                OutputStream outputStream;
                                DataOutputStream dataOutputStream;
                                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(succPort));
                                Log.i(TAG, "Requesting recover data from : " + succPort);
                                outputStream = socket.getOutputStream();
                                dataOutputStream = new DataOutputStream(outputStream);
                                dataOutputStream.write(msgRec.getBytes());
                                socket.close();

                                Socket newsocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(NEWREMOTE_PORTS.get(0)));
                                Log.i(TAG, "Requesting recover data from : " + NEWREMOTE_PORTS.get(0));
                                outputStream = newsocket.getOutputStream();
                                dataOutputStream = new DataOutputStream(outputStream);
                                dataOutputStream.write(msgRec.getBytes());
                                newsocket.close();

                                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(NEWREMOTE_PORTS.get(NEWREMOTE_PORTS.size()-3)));
                                Log.i(TAG, "Requesting recover data from : " +NEWREMOTE_PORTS.get(NEWREMOTE_PORTS.size()-3) );
                                outputStream = socket.getOutputStream();
                                dataOutputStream = new DataOutputStream(outputStream);
                                dataOutputStream.write(msgRec.getBytes());
                                socket.close();


                            } else {

                                Socket socket;
                                OutputStream outputStream;
                                DataOutputStream dataOutputStream;
                                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(succPort));
                                Log.i(TAG, "Requesting recover data from : " + succPort);
                                outputStream = socket.getOutputStream();
                                dataOutputStream = new DataOutputStream(outputStream);
                                dataOutputStream.write(msgRec.getBytes());
                                socket.close();

                                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(NEWREMOTE_PORTS.get(i + 1)));
                                Log.i(TAG, "Requesting recover data from : " + NEWREMOTE_PORTS.get(i+1));
                                outputStream = socket.getOutputStream();
                                dataOutputStream = new DataOutputStream(outputStream);
                                dataOutputStream.write(msgRec.getBytes());
                                socket.close();
                                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(predPort));
                                Log.i(TAG, "Requesting recover data from : " +predPort );
                                outputStream = socket.getOutputStream();
                                dataOutputStream = new DataOutputStream(outputStream);
                                dataOutputStream.write(msgRec.getBytes());
                                socket.close();
                            }
                        }
                    }


                }
                catch(Exception e){
                    e.getMessage();

                }


            }
            return null;
        }

    }




    private String storeCheck(String input) {
        Log.i(TAG, "Enters storeCheck method");
        String positionNode = "";
        try {
            String hashedKey = genHash(input);
            //String value = values.getAsString("value");
            String portHash = "";
            // String nextPortHash = "";
            String prevPortHash = "";


            for (int k = 0; k < NEWREMOTE_PORTS.size(); k++) {

               /* else if( k == NEWREMOTE_PORTS.size()-1)
                {
                    portHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k)) / 2 + "");
                    hashedKey = genHash(input);
                    nextPortHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(0)) / 2 + "");
                    prevPortHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k-1)) / 2 + "");
                }*/
                if(k==0)
                {
                    Log.i(TAG, "Checking if position for :"+input);
                    portHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k)) / 2 + "");
                    hashedKey = genHash(input);
                    // nextPortHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k + 1)) / 2 + "");
                    prevPortHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(NEWREMOTE_PORTS.size()-1)) / 2 + "");

                    if (portHash.compareTo(prevPortHash) <= 0 && hashedKey.compareTo(prevPortHash) > 0 && hashedKey.compareTo(portHash) >= 0) {
                        try {
                            positionNode = NEWREMOTE_PORTS.get(k)+ "::"+NEWREMOTE_PORTS.get(k+1)+"::"+NEWREMOTE_PORTS.get(k+2);
                            //Log.i(TAG, "replica position node found for query of key :"+input+ "is -:" + NEWREMOTE_PORTS.get(k+2));


                        } catch (Exception e) {
                            e.getMessage();
                        }

                    } else if (portHash.compareTo(prevPortHash) <= 0 && hashedKey.compareTo(prevPortHash) < 0 && hashedKey.compareTo(portHash) <= 0) {
                        try {
                            positionNode = NEWREMOTE_PORTS.get(k)+ "::"+NEWREMOTE_PORTS.get(k+1)+"::"+NEWREMOTE_PORTS.get(k+2);
                            //Log.i(TAG, "replica position node found for query of key :"+input+ "is -:" + NEWREMOTE_PORTS.get(k+2));

                        } catch (Exception e) {
                            e.getMessage();
                        }

                    } else if (hashedKey.compareTo(prevPortHash) > 0 && hashedKey.compareTo(portHash) <= 0) {
                        try {
                            positionNode = NEWREMOTE_PORTS.get(k)+ "::"+NEWREMOTE_PORTS.get(k+1)+"::"+NEWREMOTE_PORTS.get(k+2);
                            //Log.i(TAG, "replica position node found for query of key :"+input+ "is -:" + NEWREMOTE_PORTS.get(k+2));
                        } catch (Exception e) {
                            e.getMessage();
                        }
                    }
                }
                else if(k==4)
                {
                    Log.i(TAG, "Checking if position for :"+input);
                    portHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k)) / 2 + "");
                    hashedKey = genHash(input);
                    // nextPortHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k + 1)) / 2 + "");
                    prevPortHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k-1)) / 2 + "");

                    if (portHash.compareTo(prevPortHash) <= 0 && hashedKey.compareTo(prevPortHash) > 0 && hashedKey.compareTo(portHash) >= 0) {
                        try {
                            //positionNode = NEWREMOTE_PORTS.get(1);
                            positionNode = NEWREMOTE_PORTS.get(k)+ "::"+NEWREMOTE_PORTS.get(0)+"::"+NEWREMOTE_PORTS.get(1);
                            //Log.i(TAG, "replica position node found for query of key :"+input+ "is -:" + NEWREMOTE_PORTS.get(1));


                        } catch (Exception e) {
                            e.getMessage();
                        }

                    } else if (portHash.compareTo(prevPortHash) <= 0 && hashedKey.compareTo(prevPortHash) < 0 && hashedKey.compareTo(portHash) <= 0) {
                        try {
                            positionNode = NEWREMOTE_PORTS.get(k)+ "::"+NEWREMOTE_PORTS.get(0)+"::"+NEWREMOTE_PORTS.get(1);
                            //Log.i(TAG, "replica position node found for query of key :"+input+ "is -:" + NEWREMOTE_PORTS.get(1));

                        } catch (Exception e) {
                            e.getMessage();
                        }

                    } else if (hashedKey.compareTo(prevPortHash) > 0 && hashedKey.compareTo(portHash) <= 0) {
                        try {
                            positionNode = NEWREMOTE_PORTS.get(k)+ "::"+NEWREMOTE_PORTS.get(0)+"::"+NEWREMOTE_PORTS.get(1);
                            //Log.i(TAG, "replica position node found for query of key :"+input+ "is -:" + NEWREMOTE_PORTS.get(1));
                        } catch (Exception e) {
                            e.getMessage();
                        }
                    }
                }
                else if(k==3)
                {
                    Log.i(TAG, "Checking if position for :"+input);
                    portHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k)) / 2 + "");
                    //hashedKey = genHash(input);
                    //nextPortHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k + 1)) / 2 + "");
                    prevPortHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k-1)) / 2 + "");

                    if (portHash.compareTo(prevPortHash) <= 0 && hashedKey.compareTo(prevPortHash) > 0 && hashedKey.compareTo(portHash) >= 0) {
                        try {
                            //positionNode = NEWREMOTE_PORTS.get(0);
                            positionNode = NEWREMOTE_PORTS.get(k)+ "::"+NEWREMOTE_PORTS.get(k+1)+"::"+NEWREMOTE_PORTS.get(0);
                            //Log.i(TAG, "replica position node found for query of key :"+input+ "is -:" + NEWREMOTE_PORTS.get(0));


                        } catch (Exception e) {
                            e.getMessage();
                        }

                    } else if (portHash.compareTo(prevPortHash) <= 0 && hashedKey.compareTo(prevPortHash) < 0 && hashedKey.compareTo(portHash) <= 0) {
                        try {
                            positionNode = NEWREMOTE_PORTS.get(k)+ "::"+NEWREMOTE_PORTS.get(k+1)+"::"+NEWREMOTE_PORTS.get(0);
                            //Log.i(TAG, "replica position node found for query of key :"+input+ "is -:" + NEWREMOTE_PORTS.get(0));

                        } catch (Exception e) {
                            e.getMessage();
                        }

                    } else if (hashedKey.compareTo(prevPortHash) > 0 && hashedKey.compareTo(portHash) <= 0) {
                        try {
                            positionNode = NEWREMOTE_PORTS.get(k)+ "::"+NEWREMOTE_PORTS.get(k+1)+"::"+NEWREMOTE_PORTS.get(0);
                            //Log.i(TAG, "replica position node found for query of key :"+input+ "is -:" + NEWREMOTE_PORTS.get(0));
                        } catch (Exception e) {
                            e.getMessage();
                        }
                    }
                }
                else
                {
                    Log.i(TAG, "Checking if position for :"+input);
                    portHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k)) / 2 + "");
                    //hashedKey = genHash(input);
                    //nextPortHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k + 1)) / 2 + "");
                    prevPortHash = genHash(Integer.parseInt(NEWREMOTE_PORTS.get(k-1)) / 2 + "");

                    if (portHash.compareTo(prevPortHash) <= 0 && hashedKey.compareTo(prevPortHash) > 0 && hashedKey.compareTo(portHash) >= 0) {
                        try {
                            //positionNode = NEWREMOTE_PORTS.get(k+2);
                            positionNode = NEWREMOTE_PORTS.get(k)+ "::"+NEWREMOTE_PORTS.get(k+1)+"::"+NEWREMOTE_PORTS.get(k+2);
                            //Log.i(TAG, "replica position node found for query of key :"+input+ "is -:" + NEWREMOTE_PORTS.get(k+2));


                        } catch (Exception e) {
                            e.getMessage();
                        }

                    } else if (portHash.compareTo(prevPortHash) <= 0 && hashedKey.compareTo(prevPortHash) < 0 && hashedKey.compareTo(portHash) <= 0) {
                        try {
                            //positionNode = NEWREMOTE_PORTS.get(k+2);
                            positionNode = NEWREMOTE_PORTS.get(k)+ "::"+NEWREMOTE_PORTS.get(k+1)+"::"+NEWREMOTE_PORTS.get(k+2);
                            //Log.i(TAG, "replica position node found for query of key :"+input+ "is -:" + NEWREMOTE_PORTS.get(k+2));

                        } catch (Exception e) {
                            e.getMessage();
                        }

                    } else if (hashedKey.compareTo(prevPortHash) > 0 && hashedKey.compareTo(portHash) <= 0) {
                        try {
                            positionNode = NEWREMOTE_PORTS.get(k)+ "::"+NEWREMOTE_PORTS.get(k+1)+"::"+NEWREMOTE_PORTS.get(k+2);
                            //Log.i(TAG, "replica position node found for query of key :"+input+ "is -:" + NEWREMOTE_PORTS.get(k+2));
                        } catch (Exception e) {
                            e.getMessage();
                        }
                    }
                }

            }

        }
        catch (Exception e)
        {
            e.getMessage();
        }
        return positionNode;
    }



}