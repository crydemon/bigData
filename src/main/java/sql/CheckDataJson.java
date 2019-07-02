package sql;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
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
import org.junit.jupiter.api.Test;


public class CheckDataJson {

  @Test
  public void test1() throws IOException {
    final String queryText = FileUtils.readFileToString(new File("d:/devices.csv"), "UTF-8");
    String[] strings = queryText.split("(\\r\\n)+");
    for (String s : strings) {
      System.out.println(s);
    }
  }

  public static void main(String[] args) throws IOException {
    File d = new File("output/druid");
    d.mkdirs();
    String queryFileName = "src/main/resources/druidQuery/1142.json";
    String outputFileName = "d:/flash_sale.csv";
    pullData(queryFileName, outputFileName);
  }

  @Test
  public void test2() throws IOException {

    String queryFileName = "src/main/resources/druidQuery/1111.json";
    String outputFileName = "d:/app_start.csv";
    String paramName = "777777777";
    String paramFile = "d:/new_user_devices.csv";
    pullDataUseParam(queryFileName, outputFileName, paramName, paramFile);
  }

  @Test
  public void test4() throws IOException, ParseException {

    String queryFileName = "src/main/resources/druidQuery/1142.json";
    String outputFileName = "d:/flash_sale.csv";
    pullDataUseStartEnd(queryFileName, outputFileName);
  }

  private static void pullDataUseStartEnd(String queryFileName, String outputFileName)
      throws IOException, ParseException {
    File file = new File(queryFileName);
    final String queryJson = FileUtils.readFileToString(file, "UTF-8");
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    String start = "2019-06-28";
    Date dt = sdf.parse(start);
    Calendar rightNow = Calendar.getInstance();
    rightNow.setTime(dt);
    rightNow.add(Calendar.DATE, +1);
    String end = sdf.format(rightNow.getTime());
    while (end.compareTo("2019-06-29") <= 0) {
      String queryText = queryJson.replace("start/end", start + "/" + end);

      JSONArray druidData = queryDruidByJson(queryText);
      System.out.println(queryText);
      extractFieldFromJson(outputFileName, druidData, "event");
      start = end;
      rightNow.add(Calendar.DATE, +1);
      end = sdf.format(rightNow.getTime());
    }
  }


  private static void pullDataUseParam(String queryFileName, String outputFileName,
      String paramName, String paramFile) throws IOException {
    FileUtils.deleteQuietly(new File(outputFileName));
    File file = new File(queryFileName);
    final String queryJson = FileUtils.readFileToString(file, "UTF-8");
    final String queryText = FileUtils.readFileToString(new File(paramFile), "UTF-8");
    String[] strings = queryText.split("(\\r\\n)+");

    ArrayList<String> params = new ArrayList<>();
    for (int i = 1; i < strings.length; i++) {
      if (!strings[i].contains("\"")) {
        String param = "\"" + strings[i] + "\"";
        params.add(param);
      }
      if (i % 5000 == 0 || i == strings.length - 1) {
        System.out.println(i);
        String curQuery = queryJson.replace(paramName, String.join(",", params));
        //System.out.println(curQuery);
        JSONArray druidData = queryDruidByJson(curQuery);
        extractFieldFromJson(outputFileName, druidData, "event");
        //writeToCsv(csvText, outputFileName);
        params = new ArrayList<>();
      }
    }
  }

  @Test
  public void test3() throws IOException {

    String queryFileName = "src/main/resources/druidQuery/4368.json";
    String outputFileName = "d:/web_mob_total.csv";
    pullData(queryFileName, outputFileName);
  }

  private static void pullData(String queryFileName, String outputFileName) throws IOException {
    System.out.println("goods");
    File file = new File(queryFileName);
    final String queryText = FileUtils.readFileToString(file, "UTF-8");

    JSONArray druidData = queryDruidByJson(queryText);
    if (queryText.contains("timeseries")) {
      System.out.println("timeSeries");
      extractFieldFromJson(outputFileName, druidData, "result");
    } else if (queryText.contains("groupBy")) {
      System.out.println("groupBy");
      extractFieldFromJson(outputFileName, druidData, "event");
    }
  }


  private static String JoinStringParam(String paramFile) {
    String result = "";
    try {
      final String params = FileUtils.readFileToString(new File(paramFile), "UTF-8");
      result = params.replaceAll("(\\r\\n)+", "','");
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      return result;
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


  public static void extractFieldFromJson(String fileName, JSONArray jsonArray, String queryResult) throws IOException {
    if(jsonArray.isEmpty()) return;
    String content = "event_date";
    if (!new File(fileName).exists()) {
      Iterator<String> keys = jsonArray.getJSONObject(0).getJSONObject(queryResult).keys();
      while (keys.hasNext()) {
        content += "," + keys.next();
      }
      content += "\n";
    } else {
      content = "";
    }
    File csv = new File(fileName);//CSV文件
    BufferedWriter bw = new BufferedWriter(new FileWriter(csv, true));
    bw.write(content);

    for (int i = 0; i < jsonArray.length(); i++) {
      JSONObject event = jsonArray.getJSONObject(i).getJSONObject(queryResult);
      Iterator<String> keys = jsonArray.getJSONObject(0).getJSONObject(queryResult).keys();
      String line = jsonArray.getJSONObject(i).getString("timestamp");
      while (keys.hasNext()) {
        line += "," + Optional.ofNullable(event.get(keys.next())).orElse(0);
      }
      content = line + "\n";
      bw.write(content);
    }
    bw.flush();
    bw.close();
  }
}