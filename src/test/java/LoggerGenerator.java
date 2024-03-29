
import org.apache.log4j.Logger;

/**
 * @author YuZhansheng
 * @desc  模拟日志产生
 * @create 2019-02-25 9:56
 */
public class LoggerGenerator {

    private static Logger logger = Logger.getLogger(LoggerGenerator.class.getName());

    public static void main(String[] args) throws InterruptedException {
        int index = 0;
        while (true){
            Thread.sleep(1000);
            logger.info("current value is: " + index++);
        }
    }
}
