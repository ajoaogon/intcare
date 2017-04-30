package pt.chp.intcare.datamgmt.storm.utils;

import org.apache.commons.lang.StringUtils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DataUtils {


    public static Date parseStringToDate(String source, String pattern) throws ParseException {
        DateFormat formatter = new SimpleDateFormat(pattern);

        return formatter.parse(source);
    }

    public static String parseDateToString(Date source, String pattern) {
        DateFormat formatter = new SimpleDateFormat(pattern);

        return formatter.format(source);
    }

    public static String parseEmptyToNull(String source) {
        if (StringUtils.isNotEmpty(source))
            return source;

        return null;
    }
}
