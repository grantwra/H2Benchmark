package com.example.h2benchmark;

import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class Queries {

    JSONObject workloadJsonObject;
    static Context context;
    Utils utils;
    //Double SELECT;
    //Double UPDATE;
    //Double INSERT;
    //Double DELETE;

    public Queries(Context inContext){
        utils = new Utils();
        workloadJsonObject = Utils.workloadJsonObject;
        context = inContext;
    }

    public int startQueries(){

        utils.putMarker("{\"EVENT\":\"TESTBENCHMARK\"}", "trace_marker");

        utils.putMarker("START: App started\n", "trace_marker");
        utils.putMarker("{\"EVENT\":\"H2_START\"}", "trace_marker");

        int tester = H2Queries();
        if (tester != 0){
            return 1;
        }

        utils.putMarker("{\"EVENT\":\"H2_END\"}", "trace_marker");
        utils.putMarker("END: app finished\n", "trace_marker");

        /*
        try {
            File file2 = new File(context.getFilesDir().getPath() + "/percentage");
            FileOutputStream fos2 = context.openFileOutput(file2.getName(), Context.MODE_APPEND);
            fos2.write(("SELECT: " + (SELECT / 1800) * 100 + "%\n").getBytes());
            fos2.write(("UPDATE: " + (UPDATE / 1800) * 100 + "%\n").getBytes());
            fos2.write(("INSERT: " + (INSERT / 1800) * 100 + "%\n").getBytes());
            fos2.write(("DELETE: " + (DELETE / 1800) * 100 + "%\n").getBytes());
            fos2.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        */
        return 0;
    }

    private static int H2Queries(){

        //Connection con = utils.jdbcConnectionH2("H2Benchmark");
        Connection con = Utils.jdbcConnectionH2("H2Benchmark");
        Statement stmt;
        int sqlException = 0;
        int counter = 0;

        try {
            //JSONArray benchmarkArray = workloadJsonObject.getJSONArray("benchmark");
            JSONArray benchmarkArray = Utils.workloadJsonObject.getJSONArray("benchmark");
            for(int i = 0; i < benchmarkArray.length(); i ++){
                JSONObject operationJson = benchmarkArray.getJSONObject(i);
                Object operationObject = operationJson.get("op");
                String operation = operationObject.toString();
                //Connection con = Utils.jdbcConnectionH2("H2Benchmark");
                switch (operation) {
                    case "query": {
                        //double startBdb;
                        //double endBdb;
                        //long memBeforeQuery;
                        //long memAfterQuery;
                        sqlException = 0;
                        Object queryObject = operationJson.get("sql");
                        String query = queryObject.toString();

                        try {

                            stmt = con.createStatement();
                            //startBdb = System.currentTimeMillis();
                            //memBeforeQuery = utils.memoryAvailable(context);
                            if(query.contains("UPDATE")){
                                counter++;
                                int tester = stmt.executeUpdate(query);
                                if(tester == 0 || tester < 0){
                                    stmt.close();
                                    //con.close();
                                    throw new SQLiteException(Integer.toString(tester));

                                }
                                stmt.close();
                                //con.commit();
                                /*
                                if((counter % 50) == 0) {
                                    Statement stmt2 = con.createStatement();
                                    stmt2.execute("CHECKPOINT SYNC");
                                    stmt2.close();
                                }
                                */
                                //con.close();
                            }
                            else {
                                counter++;
                                Boolean test = stmt.execute(query);
                                //memAfterQuery = utils.memoryAvailable(context);
                                //endBdb = System.currentTimeMillis();
                                stmt.close();

                                //if(query.contains("INSERT")){
                                /*
                                if((counter % 50) == 0) {
                                    Statement stmt2 = con.createStatement();
                                    stmt2.execute("CHECKPOINT SYNC");
                                    stmt2.close();
                                }*/
                                //}


                                //con.close();

                                if (!test) {
                                    stmt.close();
                                    //con.close();
                                    throw new SQLiteException(query);

                                }
                            }
                            //double delta = endBdb - startBdb;
                            //double elapsedSeconds = delta / 1000.00000;
                            //File file = new File(context.getFilesDir().getPath() + "/testH2");
                            //FileOutputStream fos = context.openFileOutput(file.getName(), Context.MODE_APPEND);

                            /*
                            File file = new File(context.getFilesDir().getPath() + "/testH2");
                            FileOutputStream fos = context.openFileOutput(file.getName(), Context.MODE_APPEND);
                            fos.write((query + "\n").getBytes());
                            fos.close();
                            */
                            /*
                            File file2 = new File(context.getFilesDir().getPath() + "/MemoryBDB");
                            FileOutputStream fos2;
                            fos2 = context.openFileOutput(file2.getName(), Context.MODE_APPEND);
                            fos2.write(("B Available: " + memBeforeQuery + "\n").getBytes());
                            fos2.write(("B Available: " + memAfterQuery + '\n').getBytes());
                            fos2.close();
                            */

                        }
                        catch (SQLiteException e){
                            sqlException = 1;

                            /*
                            File file = new File(context.getFilesDir().getPath() + "/failedtestH2");
                            FileOutputStream fos = context.openFileOutput(file.getName(), Context.MODE_APPEND);
                            fos.write((e + "\n").getBytes());
                            fos.close();
                            */


                            continue;
                        } catch (SQLException e) {
                            sqlException = 1;

                            /*
                            File file = new File(context.getFilesDir().getPath() + "/failedtestH2");
                            FileOutputStream fos = context.openFileOutput(file.getName(), Context.MODE_APPEND);
                            fos.write((e + "\n").getBytes());
                            fos.close();
                            */

                            e.printStackTrace();
                            continue;
                        }
                        break;
                    }
                    case "break": {

                        if(sqlException == 0) {
                            Object breakObject = operationJson.get("delta");
                            int breakTime = Integer.parseInt(breakObject.toString());
                            //int tester = utils.sleepThread(breakTime);
                            int tester = Utils.sleepThread(breakTime);
                            if(tester != 0){
                                return 1;
                            }

                        }
                        sqlException = 0;
                        break;
                    }
                    default:
                        con.close();
                        return 1;
                }
                //con.close();
            }

            Statement stmt2 = con.createStatement();
            stmt2.execute("CHECKPOINT SYNC");
            stmt2.close();

            //Connection con = Utils.jdbcConnectionH2("H2Benchmark");
        } catch (JSONException e) {
            e.printStackTrace();


            try {
                con.close();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }

            return 1;
        }  /* catch (FileNotFoundException e) {
            e.printStackTrace();


            try {
                con.close();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }

            return 1;
        } catch (IOException e) {
            e.printStackTrace();


            try {
                con.close();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }

            return 1;
        } */ catch (SQLException e) {
            e.printStackTrace();
            return 1;
        }


        try {
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
            return 1;
        }

        return 0;
    }

}