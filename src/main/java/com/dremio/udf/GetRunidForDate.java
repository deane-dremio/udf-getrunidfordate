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
    	String url = null;
    	java.sql.Connection conn = null;
    	java.sql.Statement stmt = null;  
    	java.sql.ResultSet rs = null;
        
        try {
	    	System.out.println("Issuing query to Dremio to get Runid");
	        java.lang.Class.forName("com.dremio.jdbc.Driver");
	        //java.sql.Driver dremioDriver = new com.dremio.jdbc.Driver();
	        //if (dremioDriver == null ) {
	        //	throw new RuntimeException("dremioDriver is null");
	        //}
	        
	        //java.sql.DriverManager.registerDriver(dremioDriver);
	        
	        url = "jdbc:dremio:direct=localhost:31010";

	        conn = java.sql.DriverManager.getConnection(url, "dremio", "dremio123");
	        
	        if (conn == null ) {
	        	throw new RuntimeException("conn is null");
	        }
	        stmt = conn.createStatement();
	        if (stmt == null ) {
	        	throw new RuntimeException("stmt is null");
	        }
	        stmt.execute("SELECT count(*) as numNodes from sys.nodes");
	        
	        rs = stmt.getResultSet();

            if (rs == null || !rs.next()) {
            	throw new RuntimeException("rs is null");
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
        	//throw new RuntimeException("GetRunidForDate Exception: " + cnfe);
        } finally {
            rs = null;
            stmt = null;
            conn = null;
        }
    }

    public void eval() {
    	java.util.Date theDate = new java.util.Date(val.value);
    	java.text.SimpleDateFormat formatter = new java.text.SimpleDateFormat("yyyy-mm-dd");
    	try {
    	java.util.Date compareDate = formatter.parse("2010-01-01");
    	if (theDate.after(compareDate) ) {
    		out.value = 2000;
    	} else {
    		out.value = 1500;
    	}
    	}catch (Exception e) {}
    }

}

