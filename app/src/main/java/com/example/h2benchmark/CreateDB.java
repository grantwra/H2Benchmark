package com.example.h2benchmark;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;


public class CreateDB {

    Context context;

    public CreateDB(Context contextIn){
        context = contextIn;
    }

    public int create(int workload){

        Utils utils = new Utils();
        String singleJsonString = utils.jsonToString(context, workload);
        JSONObject jsonObject = utils.jsonStringToObject(singleJsonString);

        Connection con = utils.jdbcConnectionH2("H2Benchmark");
        int tester = populateH2(jsonObject, con);
        if(tester != 0){
            return 1;
        }

        return 0;
    }

    private static int populateH2(JSONObject jsonObject, Connection con){

        Statement stmt;

        try {
            JSONArray initArray = jsonObject.getJSONArray("init");
            for (int i = 0; i < initArray.length(); i++){
                stmt = con.createStatement();

                JSONObject obj2 = initArray.getJSONObject(i);
                Object sqlObject = obj2.get("sql");
                String sqlStatement = sqlObject.toString();
                stmt.execute(sqlStatement);
                stmt.close();

            }
        } catch (JSONException e) {
            e.printStackTrace();
            return 1;
        } catch (SQLException e) {
            e.printStackTrace();
            return 1;
        }

        try {
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

}
