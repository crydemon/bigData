import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.junit.jupiter.api.Test;

public class LocalDateTest {

  @Test
  public void test() {
    DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
    LocalDateTime time = LocalDateTime.now();
    String localTime = df.format(time);
    LocalDateTime ldt = LocalDateTime.parse("2017/09/28 17:07:05",df);
    System.out.println("LocalDateTime转成String类型的时间："+localTime);

  }
}
