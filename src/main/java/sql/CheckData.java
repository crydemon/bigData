package sql;


import java.io.File;
import java.io.FileInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.CDL;
import org.json.JSONArray;
import org.json.JSONException;


public class CheckData {

  public static void main(String[] args) throws IOException{
    //pullDataByUsers_id("d:\\sheet2.txt");
    //pullUsers();
    String startTime = "2019-01-05 00:00:00";
    String endTime = "2019-01-07 00:00:00";
    writeToFile(QuerySql.getUsers(startTime, endTime), "d:\\hit_login.csv");
  }


  private static void writeToFile(String sql, String fileName) throws IOException {
    CloseableHttpClient client = HttpClients.createDefault();
    Properties properties = new Properties();
    File file= new File("src/main/resources/druid");
    InputStream in = new FileInputStream(file);
    properties.load(in);
    HttpPost httpPost = new HttpPost(properties.getProperty("url"));
    Map<String, String> hashMap = new HashMap<String, String>();
    hashMap.put("query", sql);
    ObjectMapper objectMapper = new ObjectMapper();
    String json = objectMapper.writeValueAsString(hashMap);
    StringEntity entity = new StringEntity(json);
    httpPost.setEntity(entity);
    httpPost.setHeader("Accept", "application/json");
    httpPost.setHeader("Content-type", "application/json");

    CloseableHttpResponse response = client.execute(httpPost);
    HttpEntity entity1 = response.getEntity();
    String responseString = EntityUtils.toString(entity1, "UTF-8");

    //System.out.println(responseString);
    String content = Json2Csv(responseString);
    if (content == null) {
      return;
    }
    //System.out.println(content);


  }

  public static String Json2Csv(String json) throws JSONException {

    System.out.println(json);
    JSONArray jsonArray = new JSONArray(json);
    String csv = CDL.toString(jsonArray);
    return csv;
  }


}
