package sql;

import com.sun.istack.internal.NotNull;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
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
    File d = new File("output/druid");
    d.mkdirs();
    String queryFileName = "src/main/resources/druidQuery/ctr.json";
    String outputFileName = "output/druid/ctr.csv";
    String paramName = "";
    String paramFile = "";
    pullData(queryFileName, outputFileName, paramName, paramFile);
  }

  private static void pullData(String queryFileName, String outputFileName, String paramName,
      String paramFile) throws IOException {

    File file = new File(queryFileName);
    final String queryText = FileUtils.readFileToString(file, "UTF-8");

    if (!paramName.isEmpty()) {
      String intParams = JoinIntParam(paramFile);
      String queryParams = "";
      int paramCount = 0;
      for (int i = 0; i < intParams.length(); i++) {
        if (paramCount == 1000 && intParams.charAt(i) == ',') {
          String curQuery = queryText.replace(paramName, queryParams);
          JSONArray druidData = queryDruidByJson(curQuery);
          String csvText = extractFieldFromJson(outputFileName, druidData);
          writeToCsv(csvText, outputFileName);
          paramCount = 0;
        } else {
          paramCount++;
          queryParams += intParams.charAt(i);
        }
      }

    } else {
      JSONArray druidData = queryDruidByJson(queryText);
      String csvText = extractFieldFromJson(outputFileName, druidData);
      writeToCsv(csvText, outputFileName);
    }
  }

  private static void writeToCsv(String content, String fileName) {
    try {
      File csv = new File(fileName);//CSV文件
      BufferedWriter bw = new BufferedWriter(new FileWriter(csv, true));
      bw.write(content);
      bw.flush();
      bw.close();
    } catch (FileNotFoundException e) {
      //捕获File对象生成时的异常
      e.printStackTrace();
    } catch (IOException e) {
      //捕获BufferedWriter对象关闭时的异常
      e.printStackTrace();
    }
  }


  private static String JoinIntParam(String paramFile) {
    String result = "";
    try {
      final String intParams = FileUtils.readFileToString(new File(paramFile), "UTF-8");
      result = intParams.replaceAll("(\\r\\n)+", ",");
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      return result;
    }

  }


  private static JSONArray queryDruidByJson(String queryText) throws IOException {
    CloseableHttpClient client = HttpClients.createDefault();
    Properties properties = new Properties();
    //加载配置文件
    File file = new File("src/main/resources/druid");
    InputStream in = new FileInputStream(file);
    properties.load(in);
    HttpPost httpPost = new HttpPost(properties.getProperty("urlJson"));

    StringEntity entity = new StringEntity(queryText);
    httpPost.setEntity(entity);
    httpPost.setHeader("Accept", "application/json");
    httpPost.setHeader("Content-type", "application/json");
    //执行查询
    CloseableHttpResponse response = client.execute(httpPost);

    HttpEntity he = response.getEntity();
    String result = EntityUtils.toString(he, "UTF-8");
    return new JSONArray(result);
  }


  public static String extractFieldFromJson(String fileName, JSONArray jsonArray) {
    String content = "";
    if (!new File(fileName).exists()) {
      Iterator<String> keys = jsonArray.getJSONObject(0).getJSONObject("event").keys();
      while (keys.hasNext()) {
        if (content.equals("")) {
          content = keys.next();
        } else {
          content += "," + keys.next();
        }
      }
      content += "\n";
    }

    for (int i = 0; i < jsonArray.length(); i++) {
      JSONObject event = jsonArray.getJSONObject(i).getJSONObject("event");
      Iterator<String> keys = jsonArray.getJSONObject(0).getJSONObject("event").keys();
      String line = "";
      while (keys.hasNext()) {
        if (line.equals("")) {
          line = Optional.ofNullable(event.get(keys.next())).orElse(0) + ",";
        } else {
          line += "," + Optional.ofNullable(event.get(keys.next())).orElse(0) + ",";
        }
      }
      content += line + "\n";
    }
    return content;
  }
}