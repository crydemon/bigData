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
    pullDataByUsers_idByJson("d:\\users.csv");
    //pullUsers();
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
    while ((strTmp = buffReader.readLine()) != null) {
      if (strTmp.replace("\n", "").equals("")) {
        continue;
      }
      user_ids = user_ids.equals("") ? "" + Integer.valueOf(strTmp)
          : user_ids + "," + Integer.valueOf(strTmp);

      if (++i % 3000 == 0) {
        String tmp = content.replace("\"34234234\"", user_ids);
        System.out.println(tmp);
        writeToFileByJson(tmp, fileName);
        user_ids = "";
      }
    }
    String tmp = content.replace("\"34234234\"", user_ids);
    writeToFileByJson(tmp, fileName);
  }

  private static void writeToFileByJson(String json, String fileName) throws IOException {
    CloseableHttpClient client = HttpClients.createDefault();
    Properties properties = new Properties();
    File file= new File("src/main/resources/druid");
    InputStream in = new FileInputStream(file);
    properties.load(in);
    HttpPost httpPost = new HttpPost(properties.getProperty("url"));
    StringEntity entity = new StringEntity(json);
    httpPost.setEntity(entity);
    httpPost.setHeader("Accept", "application/json");
    httpPost.setHeader("Content-type", "application/json");
    CloseableHttpResponse response = client.execute(httpPost);
    HttpEntity entity1 = response.getEntity();
    //System.out.println(entity1.getContent().read());
    String responseString = EntityUtils.toString(entity1, "UTF-8");
    //System.out.println(responseString);

    JSONArray jsonArray = new JSONArray(responseString);
    String content = "";
    if (!new File(fileName).exists()) {
      content += "user_unique_id,page_code,event_date"
          + "\n";
    }

    for (int i = 0; i < jsonArray.length(); i++) {
      JSONObject event = jsonArray.getJSONObject(i).getJSONObject("event");
      //System.out.println(event.toString());
      content +=
          event.getString("user_unique_id") + ","
              + Optional.ofNullable(event.get("page_code")).orElse("unknown") + ","
              + jsonArray.getJSONObject(i).getString("timestamp").substring(0, 10)
//event.get("page_code") null ->'null'
              + "\n";
    }
    //System.out.println(content);
    if (content == null) {
      return;
    }
    CheckData.writeToCsv(content, fileName);
  }
}