
package com.telebionica.validator.ann;

import java.lang.reflect.Field;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;

/**
 *
 * @author aldo
 */
public class AnnotationUtil {

    public static boolean tieneNotNull(Field field, Class<?> grupo) {

        NotNull ann = field.getAnnotation(NotNull.class);
        if (ann == null) {
            return false;
        }

        if (grupo == null) {
            return true;
        }

        Class<?>[] group = ann.groups();

        if (group == null || group.length == 0) {
            return false;
        }

        Class<?> g = group[0];

        return grupo.isAssignableFrom(g);

    }

    public static boolean tieneSize(Field field, Class<?> grupo) {

        Size ann = field.getAnnotation(Size.class);
        if (ann == null) {
            return false;
        }

        if (grupo == null) {
            return true;
        }

        Class<?>[] group = ann.groups();

        if (group == null || group.length == 0) {
            return false;
        }

        Class<?> g = group[0];

        return grupo.isAssignableFrom(g);

    }

    public static boolean tienePatron(Field field, Class<?> grupo) {

        Pattern ann = field.getAnnotation(Pattern.class);
        if (ann == null) {
            return false;
        }

        if (grupo == null) {
            return true;
        }

        Class<?>[] group = ann.groups();

        if (group == null || group.length == 0) {
            return false;
        }

        Class<?> g = group[0];

        return grupo.isAssignableFrom(g);

    }

    public static boolean tieneUnico(Field field, Class<?> grupo) {

        Unique ann = field.getAnnotation(Unique.class);
        if (ann == null) {
            return false;
        }

        if (grupo == null) {
            return true;
        }

        Class<?>[] group = ann.groups();

        if (group == null || group.length == 0) {
            return false;
        }

        Class<?> g = group[0];

        return grupo.isAssignableFrom(g);

    }

}
