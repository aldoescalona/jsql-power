
package com.telebionica.validator;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;

/**
 *
 * @author aldo
 */
public class Brocal {

    public static Map<String, ResourceBundle> bundleMap = new HashMap();

    public static List<Messages> translate(List<Messages> messagesList, String locale) {

        for (Messages in : messagesList) {
            for (Message mensaje : in.getMessages()) {

                String msg = getUserMessage(mensaje.getKey(), locale);

                if (msg != null) {
                    try {
                        msg = MessageFormat.format(msg, mensaje.getP1(), mensaje.getP2());
                    } catch (Exception e) {
                        msg = mensaje.getKey() + " " + e.getLocalizedMessage();
                    }

                } else {
                    msg = mensaje.getMessage();
                }
                mensaje.setMessage(msg);

            }
        }
        return messagesList;
    }

    private static String getUserMessage(String key, String locale) {

        ResourceBundle messagesBundle = getBundle(locale);

        if (key == null) {
            return key;
        }

        if (!messagesBundle.containsKey(key)) {
            return key;
        }

        String msg = messagesBundle.getString(key);
        return msg;
    }

    private static ResourceBundle getBundle(String loc) {

        ResourceBundle bundle = bundleMap.get(loc);
        if (bundle == null) {
            Locale locale = new Locale(loc);
            bundle = ResourceBundle.getBundle("messages.Messages", locale);
            bundleMap.put(loc, bundle);
        }
        return bundle;
    }
}
