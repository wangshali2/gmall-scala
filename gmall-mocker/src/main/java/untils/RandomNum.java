package untils;

import java.util.Random;
//随机获取[x,y]的随机数
public class RandomNum {

    public static int getRandInt(int fromNum, int toNum) {
        return fromNum + new Random().nextInt(toNum - fromNum + 1);
    }
}
