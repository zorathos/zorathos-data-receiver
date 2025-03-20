package org.datacenter.receiver.util;

import org.datacenter.exception.ZorathosException;

/**
 * @author : [wangminan]
 * @description : 将1-20内整数转换为中文字符
 */
public class IntToChineseCharacterUtil {

    public static String intToChineseCharacter(Integer number) {
        return switch (number) {
            case 1 -> "一";
            case 2 -> "二";
            case 3 -> "三";
            case 4 -> "四";
            case 5 -> "五";
            case 6 -> "六";
            case 7 -> "七";
            case 8 -> "八";
            case 9 -> "九";
            case 10 -> "十";
            case 11 -> "十一";
            case 12 -> "十二";
            case 13 -> "十三";
            case 14 -> "十四";
            case 15 -> "十五";
            case 16 -> "十六";
            case 17 -> "十七";
            case 18 -> "十八";
            case 19 -> "十九";
            case 20 -> "二十";
            default -> throw new ZorathosException("Plane number of sorties out of range: " + number + ", must between 1-20.");
        };
    }
}
