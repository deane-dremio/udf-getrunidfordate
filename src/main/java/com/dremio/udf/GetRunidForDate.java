package com.dremio.udf;/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.dremio.api.rest.Dremio;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.sabot.exec.context.ContextInformation;
import io.netty.buffer.ArrowBuf;

import org.apache.arrow.vector.holders.DateMilliHolder;
import org.apache.arrow.vector.holders.IntHolder;
import org.apache.arrow.vector.holders.VarCharHolder;
import org.json.JSONObject;

import java.net.HttpURLConnection;
import java.net.URL;

import javax.inject.Inject;

// Dremio Function: protect("<Column Name with unencrypted values>","{Token})
@FunctionTemplate(
        name = "GetRunidForDate",
        scope = FunctionTemplate.FunctionScope.SIMPLE,
        nulls = FunctionTemplate.NullHandling.NULL_IF_NULL,
        isDynamic = true)
public class GetRunidForDate implements SimpleFunction {
    @Inject ArrowBuf buffer;

    //@Param
    //VarCharHolder val;  //The column value to encrypt
    
    @Param
    DateMilliHolder val;  //The column value to encrypt

    @Output
    IntHolder out; // The buffer containing the output to the column

    @Workspace
    Integer return_val;
    

    @Inject
    ContextInformation contextInfo;


    public void setup() {
    	
    }

    public void eval() {
    	// THIS IS THE ORACLE ROUTE
    	String url = null;
    	java.sql.Connection conn = null;
    	java.sql.Statement stmt = null;  
    	java.sql.ResultSet rs = null;
        
        try {
	    	System.out.println("Issuing query to Oracle to get Runid");
	        java.lang.Class.forName("oracle.jdbc.driver.OracleDriver");
	        
	        url = "jdbc:oracle:thin:@192.168.0.21:1521:xe";

	        conn = java.sql.DriverManager.getConnection(url, "hr", "hr");
	        System.out.println("Obtained connection");
	        if (conn == null ) {
	        	throw new RuntimeException("conn is null");
	        } else {
	        	System.out.println("conn is not null");
	        }
	        
	        System.out.println("Obtaining statement object");
	        stmt = conn.createStatement();
	        System.out.println("Obtained statement object");
	        
	        if (stmt == null ) {
	        	throw new RuntimeException("stmt is null");
	        } else {
	        	System.out.println("stmt is not null");
	        }
	        System.out.println("Executing statement");
	        rs = stmt.executeQuery("SELECT count(*) from HR.COUNTRIES");
	        System.out.println("Statement executed");

            if (rs == null || !rs.next()) {
            	throw new RuntimeException("rs is null");
            } else {
            	System.out.println("Result set is not empty");
            }
            
            return_val = Integer.valueOf(rs.getInt(1));
            
        } catch (java.sql.SQLException e) {
            
            if (rs != null)   {
                try {
                    rs.close(); 
                } catch (java.sql.SQLException ignore) { }
            }
            if (stmt != null) {
                try {
                    stmt.close(); 
                } catch (java.sql.SQLException ignore) { }
            }
            if (conn != null) {
                try {
                    conn.close(); 
                } catch (java.sql.SQLException ignore) { }
            }
            
            throw new RuntimeException("GetRunidForDate Exception: " + e);
        } catch (java.lang.ClassNotFoundException cnfe) {
        	throw new RuntimeException("GetRunidForDate Exception: " + cnfe);
        } finally {
            rs = null;
            stmt = null;
            conn = null;
        }
    	
    	// THIS IS THE REST API ROUTE
    	/*com.dremio.api.rest.Dremio dremio = new com.dremio.api.rest.Dremio("http://192.168.0.21:9047", "dremio", "dremio123");
    	java.lang.String json;
    	java.lang.String jobId;
    	java.lang.String state;
    	java.lang.String jobResult;
    	int row_count = 0;
    	
    	System.out.println("Dremio token: " + dremio.getToken());
		try {
			java.net.URL urlObj = new java.net.URL("https://192.168.0.21");
			java.net.HttpURLConnection con = (java.net.HttpURLConnection) urlObj.openConnection();
			con.setRequestMethod("GET");
                        // Set connection timeout
			con.setConnectTimeout(3000);
			con.connect();
 
			int code = con.getResponseCode();
			System.out.println("Code: " + code);

		} catch (Exception e) {
			System.out.println("Exception getting code");
		}

		
    	System.out.println("POSTING QUERY BACK TO DREMIO");
    	json = dremio.postSQL("SELECT count(*) as numNodes from sys.nodes");
    	System.out.println("QUERY POSTED, RESULT RECEIVED:" + json);
    	
    	org.json.JSONObject obj = new org.json.JSONObject(json);
		jobId = obj.getString("id").toString();
		
        while (true) {
            state = dremio.getJobStatus(jobId);
            org.json.JSONObject stateObj =  new org.json.JSONObject(state);
            
            if ("COMPLETED".equalsIgnoreCase(obj.getString("jobState").toString())) {
                row_count = obj.getInt("rowCount");
                System.out.println("row count: " + row_count);
                break;
            }
            
            if ("CANCELED".equalsIgnoreCase(obj.getString("jobState").toString()) ||
            	"FAILED".equalsIgnoreCase(obj.getString("jobState").toString())) {
                // todo add info about why did it fail
                throw new RuntimeException("job failed: " + state);
            }
            
            try {
            	Thread.sleep(1000);
            } catch (InterruptedException ie) {}
        }
        
        jobResult = dremio.getJobResults(jobId,  0,  0);
        System.out.println(jobResult);*/
		
		
    	// THIS WAS THE UNSUCCESSFUL DREMIO JDBC ROUTE
    	/*String url = null;
    	java.sql.Connection conn = null;
    	java.sql.Statement stmt = null;  
    	java.sql.ResultSet rs = null;
        
        try {
	    	System.out.println("Issuing query to Dremio to get Runid");
	        java.lang.Class.forName("com.dremio.jdbc.Driver");
	        
	        url = "jdbc:dremio:direct=192.168.0.21:31010";

	        conn = java.sql.DriverManager.getConnection(url, "dremio", "dremio123");
	        System.out.println("Obtained connection");
	        if (conn == null ) {
	        	throw new RuntimeException("conn is null");
	        } else {
	        	System.out.println("conn is not null");
	        }
	        
	        System.out.println("Obtaining statement object");
	        stmt = conn.createStatement();
	        System.out.println("Obtained statement object");
	        
	        if (stmt == null ) {
	        	throw new RuntimeException("stmt is null");
	        } else {
	        	System.out.println("stmt is not null");
	        }
	        System.out.println("Executing statement");
	        rs = stmt.executeQuery("SELECT count(*) as numNodes from sys.nodes");
	        System.out.println("Statement executed");

            if (rs == null || !rs.next()) {
            	throw new RuntimeException("rs is null");
            } else {
            	System.out.println("Result set is not empty");
            }
            
            return_val = Integer.valueOf(rs.getInt(1));
            
        } catch (java.sql.SQLException e) {
            
            if (rs != null)   {
                try {
                    rs.close(); 
                } catch (java.sql.SQLException ignore) { }
            }
            if (stmt != null) {
                try {
                    stmt.close(); 
                } catch (java.sql.SQLException ignore) { }
            }
            if (conn != null) {
                try {
                    conn.close(); 
                } catch (java.sql.SQLException ignore) { }
            }
            
            throw new RuntimeException("GetRunidForDate Exception: " + e);
        } catch (java.lang.ClassNotFoundException cnfe) {
        	throw new RuntimeException("GetRunidForDate Exception: " + cnfe);
        } finally {
            rs = null;
            stmt = null;
            conn = null;
        }*/

    	java.util.Date theDate = new java.util.Date(val.value);
    	java.text.SimpleDateFormat formatter = new java.text.SimpleDateFormat("yyyy-mm-dd");
    	try {
    	java.util.Date compareDate = formatter.parse("2010-01-01");
    	if (theDate.after(compareDate) ) {
    		out.value = return_val.intValue() * 100;
    	} else {
    		out.value = 1500;
    	}
    	}catch (Exception e) {}
    }

}

