package com.dremio.api.rest;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;

import org.json.*;

public class Dremio {

	private String host;
	//private String user;
	//private String password;
	private String token;
	
	public Dremio(String host, String user, String password) {
		this.host = host;
		//this.user = user;
		//this.password = password;
		this.token = getToken(host, user, password);
	}
	
	public String getToken() {
		return token;
	}
	
	private String doPost(String dremioURL, String bodyPayload, boolean withToken) {
		StringBuffer jsonString = new StringBuffer();
		
		try {
            URL url = new URL(dremioURL);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            
            connection.setDoInput(true);
            connection.setDoOutput(true);
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Content-Type", "application/json");
            
            if (withToken) {
            	connection.setRequestProperty("Authorization", this.token);
            }
            System.out.println("Got connection: " + connection.toString());
            OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream());
            if (bodyPayload != null) {
            	writer.write(bodyPayload);
            }
            
            System.out.println("Written payload");
            writer.close();
            BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
System.out.println("Reading response");
            String line;
            while ((line = br.readLine()) != null) {
            	System.out.println("Got a line: " + line);
                jsonString.append(line);
            }
            br.close();
            connection.disconnect();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
		
		return jsonString.toString();
	}
	
	private String doGet(String dremioURL) {
		StringBuffer jsonString = new StringBuffer();
		
		try {
            URL url = new URL(dremioURL);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            
            connection.setDoInput(true);
            connection.setDoOutput(true);
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Authorization", this.token);
            connection.setRequestProperty("cache-control", "no-cache");
            
            BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));

            String line;
            while ((line = br.readLine()) != null) {
                jsonString.append(line);
            }
            br.close();
            connection.disconnect();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
		
        return jsonString.toString();
	}
	
	private String doPut(String dremioURL) {
		return doPut(dremioURL, null);
	}
	
	private String doPut(String dremioURL, String bodyPayload) {
		StringBuffer jsonString = new StringBuffer();
		
		try {
            URL url = new URL(dremioURL);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            
            connection.setDoInput(true);
            connection.setDoOutput(true);
            connection.setRequestMethod("PUT");
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Authorization", this.token);
            connection.setRequestProperty("cache-control", "no-cache");
            
            // DEANE TODO Check if this is the right way to add a body to a PUT message
            // test it with putWlmRule. Also need to make sure we can successfully call other PUT methods that don't need a body!
            OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream());
            if (bodyPayload != null) {
            	writer.write(bodyPayload);
            }
            
            writer.close();
            
            BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));

            String line;
            while ((line = br.readLine()) != null) {
                jsonString.append(line);
            }
            br.close();
            connection.disconnect();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
		
        return jsonString.toString();
	}
	
	private String doDelete(String dremioURL) {
		StringBuffer jsonString = new StringBuffer();
		
		try {
            URL url = new URL(dremioURL);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            
            connection.setDoInput(true);
            connection.setDoOutput(true);
            connection.setRequestMethod("DELETE");
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Authorization", this.token);
            connection.setRequestProperty("cache-control", "no-cache");
            
            BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));

            String line;
            while ((line = br.readLine()) != null) {
                jsonString.append(line);
            }
            br.close();
            connection.disconnect();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
		
        return jsonString.toString();
	}
	
	private String getToken(String host, String user, String password) {
		String getTokenURL = host + "/apiv2/login";
		String payload = "{\"userName\": \"" + user + "\", \"password\": \"" + password + "\"}";
		String responseJSON = doPost(getTokenURL, payload, false);
		JSONObject obj = new JSONObject(responseJSON);
		
		return "_dremio" + obj.get("token").toString();
	}
	
	public String getUserByName(String name) {
        String url = this.host + "/api/v3/user/by-name/" + name;
        return doGet(url);
	}
	
	public String getUserById(String id) {
        String url = this.host + "/api/v3/user/" + id;
		return doGet(url);
	}

    public String getGroupByName(String name) {
        String url = this.host + "/api/v3/group/by-name/" + name;
        return doGet(url);
    }
    
    public String getGroupById(String id) {
        String url = this.host + "/api/v3/group/" + id;
        return doGet(url);
    }

    public String getCatalog() {
        /*Lists all top-level catalog containers.
        https://docs.dremio.com/rest-api/catalog/get-catalog.html*/
        String url = this.host + "/api/v3/catalog";
        return doGet(url);
    }
    

    public String  getCatalogById(String id) {
        /*Retrieves information about a specific catalog entity
        (source, space, folder, file or dataset) using it's ID.
        Child information (if applicable) of the catalog entity are
        also retrieved along with their ID, path, type, and containerType.

        https://docs.dremio.com/rest-api/catalog/get-catalog-id.html*/

        String url = this.host + "/api/v3/catalog/" +  id;
        return doGet(url);
    }

    public String getCatalogByPath(String path) {
        /*Retrieves information about a specific catalog entity
        (source, space, folder, file or dataset) using it's path.
        Child information (if applicable) of the catalog entity
        are also retrieved along with their ID, path, type, and containerType.

        https://docs.dremio.com/rest-api/catalog/get-catalog-path.html*/

        String url = this.host + "/api/v3/catalog/by-path/" + path;
        return doGet(url);
    }

    public String getCatalogCollaborationTagById(String id) {
        /*Retrieves tags for a catalog entity.

        https://docs.dremio.com/rest-api/catalog/get-catalog-collaboration.html*/

        String url = this.host + "/api/v3/catalog/" + id + "/collaboration/tag";
    	return doGet(url);
    }


    public String getCatalogCollaborationWikiById(String id) {
        /*Retrieves wiki content for a catalog entity.

        https://docs.dremio.com/rest-api/catalog/get-catalog-collaboration.html*/

        String url = this.host + "/api/v3/catalog/" + id + "/collaboration/wiki";
        return doGet(url);
    }

    public String postCatalog(String body) {
        /*Creates a new catalog entity.
        Entity must be either Space, Source, Folder or Dataset

        https://docs.dremio.com/rest-api/catalog/post-catalog.html*/

        String url = this.host + "/api/v3/catalog";
        return doPost(url, body, true);
    }
    

    public String postCatalogPromoteToPds(String id, String body) {
        /*Promotes a file or folder in a file-based source to a physical dataset
        (PDS). The supplied path is used to determine what entity is promoted.
        Files or folders inside a source can be promoted to physical datasets.
        This converts the folder/file to a dataset; the dataset then has a new
        ID since it is a new entity.

        https://docs.dremio.com/rest-api/catalog/post-catalog-id.html*/

        String url = this.host + "/api/v3/catalog/" + id;
    	return doPost(url, body, true);
    }

    public String postCatalogRefresh(String id) {
        /*Refreshes a catalog entity.

        Refreshes all the dependent reflections of the specified physical dataset.
        This endpoint only functions with physical dataset IDss and has no response object.

        https://docs.dremio.com/rest-api/catalog/post-catalog-id-refresh.html*/

        String url = this.host + "/api/v3/catalog/" + id + "/refresh";
        return doPost(url, null, true);
    }

    public String postCatalogTags(String id, String body) {
        /*Creates and updates Tags content for a catalog entity.

        https://docs.dremio.com/rest-api/catalog/post-catalog-collaboration.html*/

        String url = this.host + "/api/v3/catalog/" + id + "/collaboration/tag";
        return doPost(url, body, true);
    }

    public String postCatalogWiki(String id, String body) {
        /*Creates and updates Wiki content for a catalog entity.

        https://docs.dremio.com/rest-api/catalog/post-catalog-collaboration.html*/

        String url = this.host + "/api/v3/catalog/" + id + "/collaboration/wiki";
    	return doPost(url, body, true);
    }

    public String putCatalog(String id) {
        /*Updates existing datasets and sources.

        https://docs.dremio.com/rest-api/catalog/put-catalog-id.html*/

        String url = this.host + "/api/v3/catalog/" + id;
        return doPut(url);
    }

    public String deleteCatalog(String id, String tag) {
        /*Deletes an existing catalog entity (source, space, folder in a space,
        PDS dataset, and VDS dataset).

        If you have a file/folder in a file-based source that has been promoted
        (it's now a PDS) and then delete it, the file reverts to the original format.
        For example, if a PDS was originally a text file, it reverts back to a text
        file after the PDS is deleted.

        https://docs.dremio.com/rest-api/catalog/delete-catalog-id.html*/

        String url = this.host + "/api/v3/catalog/" + id + "?tag=" + tag;
        return doDelete(url);
    }

    public String getReflections() {
        /*Retrieves all reflections. Only users with administrator privileges can
        use the reflections API.

        https://docs.dremio.com/rest-api/reflections/get-reflection.html*/

        String url = this.host + "/api/v3/reflection";
        return doGet(url);
	}

    public String getReflectionById(String id) {
        /*Retrieves a specific reflection.
        Only users with administrator privileges can use the reflections API.

        https://docs.dremio.com/rest-api/reflections/get-reflection-id.html*/

        String url = this.host + "/api/v3/reflection/" + id;
        return doGet(url);
    }
    
    public String getReflectionSummary() {
        /*Retrieves all reflections as a summary.

        https://docs.dremio.com/rest-api/reflections/get-reflection-summary.html*/

        String url = this.host + "/api/v3/reflection/summary";
        return doGet(url);
    }
    
    public String postReflection(String body) {
        /*Creates a new reflection.

        https://docs.dremio.com/rest-api/reflections/post-reflection.html*/

        String url = this.host + "/api/v3/reflection";
        return doPost(url, body, true);
    }
    
    public String putReflection(String id) {
        /*Updates a specific reflection.

        https://docs.dremio.com/rest-api/reflections/put-reflection.html*/

        String url = this.host + "/api/v3/reflection/" + id;
        return doPut(url);
    } 
 
    public String deleteReflection(String id) {
        /*Deletes a specific reflection.

        https://docs.dremio.com/rest-api/reflections/delete-reflection.html*/

        String url = this.host + "/api/v3/reflection/" + id;
        return doDelete(url);
    }

    public String getJobStatus(String id) {
        /*Retrieves a job's status.

        https://docs.dremio.com/rest-api/jobs/get-job.html*/

        String url = this.host + "/api/v3/job/" + id;
        return doGet(url);
    }

    public String getJobResults(String id, Integer _offset, Integer _limit) {
        /*Retrieve results for a completed job.
        https://docs.dremio.com/rest-api/jobs/get-job.html*/
    	
    	int offset = (_offset == null)?0:_offset.intValue();
    	int limit = (_limit == null)?0:_limit.intValue();
        String url = this.host + "/api/v3/job/" + id + "/results";
        
        // DEANE TODO Probably going to need an override method for issueGET so that I can pass in the offset and limit parameters
        //session.params = {}
        //session.params['offset'] = offset
        //session.params['limit'] = limit
        //response = session.get(url)
        //session.params = {}
        return doGet(url);
    }

    public String postJobCancel(String id) {
        /*Cancels a running job.
        https://docs.dremio.com/rest-api/jobs/post-job.html*/

        String url = this.host + "/api/v3/job/" + id + "/cancel";
        return doPost(url, null, true);
    }

    public String getWlmQueue() {
        /*Retrieves list of queues.
        https://docs.dremio.com/rest-api/wlm/get-wlm-queue.html*/

        String url = this.host + "/api/v3/wlm/queue";
        return doGet(url);
    }

    public String getWlmQueueByName(String name) {
        /*Retrieves information about a specific queue by name.
        https://docs.dremio.com/rest-api/wlm/get-wlm-queue.html*/

        String url = this.host + "/api/v3/wlm/queue/by-name/" + name;
        return doGet(url);
    }

    public String getWlmQueueById(String id) {
        /*Retrieves information about a specific queue by ID.
        https://docs.dremio.com/rest-api/wlm/get-wlm-queue.html*/

        String url = this.host + "/api/v3/wlm/queue/" + id;
        return doGet(url);
    }

    public String postWlmQueue(String body) {
        /*Creates a new queue.
        https://docs.dremio.com/rest-api/wlm/post-wlm-queue.html*/

        String url = this.host + "/api/v3/wlm/queue";
        return doPost(url, null, true);
    }

    public String putWlmQueue(String id) {
        /*Updates a queue's attributes by ID.
        https://docs.dremio.com/rest-api/wlm/put-wlm-queue.html*/

        String url = this.host + "/api/v3/wlm/queue/" + id;
        return doPut(url);
    }

    public String deleteWlmQueue(String id) {
        /*Deletes a specific queue by ID.
        https://docs.dremio.com/rest-api/wlm/delete-wlm-queue.html*/

        String url = this.host + "/api/v3/wlm/queue/" + id;
        return doDelete(url);
    }

    public String getWlmRule() {
        /*Retrieves a list of rules.
        https://docs.dremio.com/rest-api/wlm/get-wlm-rule.html*/

        String url = this.host + "/api/v3/wlm/rule";
        return doGet(url);
    }

    public String putWlmRule(String body) {
        /*Creates new rules, updates existing rules, and deletes rules.
        A ruleset is an array of rules where order matters.
        The most high priority rule is first, and so on.
        Through the WLM REST API, you interact with the ruleset, not any individual rules.

        When you remove a rule from the ruleset (the array of rules) list, it is deleted.

        https://docs.dremio.com/rest-api/wlm/put-wlm-queue.html*/

        String url = this.host + "/api/v3/wlm/rule";
        return doPut(url, body);
    }

    public String getSource() {
        /*Lists all sources.
        https://docs.dremio.com/rest-api/sources/get-source.html*/

        String url = this.host + "/api/v3/source";
        return doGet(url);
    }

    public String getSourceById(String id) {
        /*Lists all sources.
        https://docs.dremio.com/rest-api/sources/get-source.html*/

        String url = this.host + "/api/v3/source/" + id;
        return doGet(url); 
    }

    public String postSource(String body) {
        /*Creates a new source.
        https://docs.dremio.com/rest-api/sources/post-source.html*/

        String url = this.host + "/api/v3/source";
        return doPost(url, body, true);
    }

    public String putSource(String id, String body) {
        /*Updates an existing source
        https://docs.dremio.com/rest-api/sources/put-source.html*/

        String url = this.host + "/api/v3/source/" + id;
        return doPut(url, body);
    }

    public String deleteSource(String id) {
        /*Deletes a specific queue by ID.
        https://docs.dremio.com/rest-api/sources/delete-source.html*/

        String url = this.host + "/api/v3/source/" + id;
        return doDelete(url);
    }

    public String postSQL(String sql) {
        /*Submits a SQL query.
        https://docs.dremio.com/rest-api/sources/post-source.html*/

        String url = this.host + "/api/v3/sql";
        String body = "{\"sql\": \"" + sql + "\"}";
        return doPost(url, body, true);
    }

    public String getVote() {
        /*List all votes as a summary.
        Only users with administrator privileges can use this endpoint.

        https://docs.dremio.com/rest-api/votes/get-vote.html*/

        String url = this.host + "/api/v3/vote";
        return doGet(url);
    }

    public String getServerStatus() {
        /*Retrieves the status of a node. For high availability,
        this API can be used to determine whether a controller node is active or not.

        https://docs.dremio.com/rest-api/get-server_status.html*/

        String url = this.host + "/apiv2/server_status";
        return doGet(url);
    }

}
