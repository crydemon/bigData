package sql;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Optional;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;


public class CheckDataJson {

  public static void main(String[] args) throws IOException {
    //pullDataByUsers_idByJson("d:\\user_druid.csv");
    pullData();
  }

  public static void pullData() throws IOException {
    File file = new File("src/main/resources/druidQueryJson/4050_exposure_uv.json");
    final String content = FileUtils.readFileToString(file, "UTF-8");
    String fileName = "d:\\druidRecord.csv";
    writeToFileByJson(content, fileName);
  }


  public static void pullDataByUsers_idByJson(String filePath) throws IOException {
    FileInputStream fin = new FileInputStream(filePath);
    InputStreamReader reader = new InputStreamReader(fin);
    BufferedReader buffReader = new BufferedReader(reader);
    String user_ids = "";
    String strTmp;
    String fileName = "d:\\druidRecord.csv";
    File file = new File("d:\\scanByUserId.json");
    final String content = FileUtils.readFileToString(file, "UTF-8");
    int i = 0;
//    while ((strTmp = buffReader.readLine()) != null) {
//      if (strTmp.replace("\n", "").equals("")) {
//        continue;
//      }
//      if(strTmp.matches("[a-zA-z]*")) {
//        continue;
//      }
//      user_ids = user_ids.equals("") ? "" + Integer.valueOf(strTmp)
//          : user_ids + "," + Integer.valueOf(strTmp);
//
//      if (++i % 3000 == 0) {
//        String tmp = content.replace("\"tmp_pos\"", user_ids);
//        System.out.println(tmp);
//        writeToFileByJson(tmp, fileName);
//        user_ids = "";
//      }
//    }
    String tmp = content.replace("\"tmp_pos\"", user_ids);
    writeToFileByJson(tmp, fileName);
  }

  private static void writeToFileByJson(String json, String fileName) throws IOException {
    CloseableHttpClient client = HttpClients.createDefault();
    Properties properties = new Properties();
    File file = new File("src/main/resources/druid");
    InputStream in = new FileInputStream(file);
    properties.load(in);
    HttpPost httpPost = new HttpPost(properties.getProperty("urlJson"));
    StringEntity entity = new StringEntity(json);
    httpPost.setEntity(entity);
    httpPost.setHeader("Accept", "application/json");
    httpPost.setHeader("Content-type", "application/json");
    CloseableHttpResponse response = client.execute(httpPost);
    HttpEntity entity1 = response.getEntity();
    String responseString = EntityUtils.toString(entity1, "UTF-8");

    JSONArray jsonArray = new JSONArray(responseString);
    String content = extractFieldFromJson_greater100uv(fileName, jsonArray);
    CheckData.writeToCsv(content, fileName);
  }

  public static String extractFieldFromJson(String fileName, JSONArray jsonArray) {
    String content = "";
    if (!new File(fileName).exists()) {
      content = "goods_id," + "clicks," + "impressions," + "ctr" + "\n";
    }
    for (int i = 0; i < jsonArray.length(); i++) {
      JSONObject event = jsonArray.getJSONObject(i).getJSONObject("event");
      content += event.getString("goods_id") + ","
          + Optional.ofNullable(event.get("sum_clicks")).orElse(0) + ","
          + Optional.ofNullable(event.get("sum_impressions")).orElse(0) + ","
          + Optional.ofNullable(event.get("ctr")).orElse(0) + ","
          + "\n";
    }
    return content;
  }

  public static String extractFieldFromJson_greater100uv(String fileName, JSONArray jsonArray) {
    String content = "";
    if(jsonArray.isEmpty()) return content;
    if (!new File(fileName).exists()) {
      content = "goods_id," + "exposure_uv" + "exposure_pv" + "\n";
    }
    jsonArray.getJSONObject(0).getJSONObject("event").keys();
    for (int i = 0; i < jsonArray.length(); i++) {
      JSONObject event = jsonArray.getJSONObject(i).getJSONObject("event");
      content += Optional.ofNullable(event.get("page_goods_id")).orElse(0) + ","
          + Optional.ofNullable(event.get("exposure_uv")).orElse(0) + ","
          + Optional.ofNullable(event.get("exposure_pv")).orElse(0) + ","
          + "\n";
    }
    return content;
  }
}