package tw.com.wd.hbase.util.hbasebuffer.util;

import java.lang.reflect.Field;

public class ObjectUtils {
    public static <TARGET, INJECTOR> void injectObject(TARGET target, INJECTOR injector) {
        Class<?> injectorClass = injector.getClass();
        boolean isFound = false;

        for (Field f : target.getClass().getDeclaredFields()) {
            if (f.getType().isAssignableFrom(injectorClass)) {
                setField(f, target, injector);
                isFound = true;
            }
        }

        if (!isFound) {
            for (Field f : target.getClass().getSuperclass().getDeclaredFields()) {
                if (f.getType().isAssignableFrom(injectorClass)) {
                    setField(f, target, injector);
                    isFound = true;
                }
            }
        }
    }

    private static void setField(Field f, Object obj, Object value) {
        if (!f.isAccessible()) {
            f.setAccessible(true);
        }

        try {
            f.set(obj, value);
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }
}
