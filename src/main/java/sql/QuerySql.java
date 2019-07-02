package sql;

public class QuerySql {

  public static String getUsers(String startTime, String endTime) {
    String sql = "SELECT user_unique_id "
        + "FROM hit  "
        + "WHERE  __time >= TIMESTAMP '" + startTime + "' "
        + "and __time <= TIMESTAMP '" + endTime + "' "
        + "group by user_unique_id"
        ;
    return sql;
  }

  public static String getFemaleRate(String startTime, String endTime) {
    String sql = "SELECT goods_id, "
        + "sum(impressions) as impressions, "
        + "sum(clicks) as clicks "
        + "FROM ctr  "
        + "WHERE  __time >= TIMESTAMP '" + startTime + "' "
        + "and __time <= TIMESTAMP '" + endTime + "' "
        + "and gender = 'female' "
        + "group by goods_id ";
    return sql;
  }

  public static String getMaleRate(String startTime, String endTime) {
    String sql = "SELECT goods_id, "
        + "sum(impressions) as impressions, "
        + "sum(clicks) as clicks "
        + "FROM ctr  "
        + "WHERE  __time >= TIMESTAMP '" + startTime + "' "
        + "and __time <= TIMESTAMP '" + endTime + "' "
        + "and gender = 'male' "
        + "group by goods_id";
    return sql;
  }

  public static String getUsers(String startTime, String endTime, String users_id) {
    String sql = "SELECT __time, user_unique_id "
        + "FROM hit  "
        + "WHERE  __time >= TIMESTAMP '" + startTime + "' "
        + "and __time <= TIMESTAMP '" + endTime + "' "
        + "and user_unique_id IN (" + users_id + ") "
        + "and event_name IN ('common_click', 'screen_view', 'page_view') ";
    return sql;
  }

  public static String getNameTracker(String startTime, String endTime) {
    String sql = "SELECT hour(__time),count(*) "
        + "FROM hit  "
        + "WHERE  __time >= TIMESTAMP '" + startTime + "' "
        + "and __time <= TIMESTAMP '" + endTime + "' "
        + "and name_tracker = 'vova_h5'  "
        + "group by hour(__time)";
    return sql;
  }
  public static String searchCTR(String startTime, String endTime) {
    String sql = "SELECT\n"
        + " floor(__time to day) AS cur_day,\n"
        + " sum(impressions) AS sum_impressions,\n"
        + " sum(clicks) AS sum_clicks,\n"
        + " count(DISTINCT domain_userid) AS uv,\n"
        + " sum(clicks) * 1.0/sum(impressions) AS rate\n"
        + "FROM goods_ctr_v2\n"
        + "WHERE  page_code = 'search_result'\n"
        + "    AND __time >= TIMESTAMP '" + startTime + "'\n"
        + "group BY floor(__time to day)";
    return sql;
  }
}
