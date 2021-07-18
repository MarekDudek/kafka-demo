package md;

import lombok.SneakyThrows;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.stream.Stream;

public enum PropertiesHelper
{
    ;

    public static Properties fromFile(final String... names)
    {
        final Properties p = new Properties();
        Stream.of(names).forEach(name -> {
            load(p, name);
        });
        return p;
    }

    @SneakyThrows
    private static void load(final Properties p, final String name)
    {
        final FileInputStream s = new FileInputStream(name);
        p.load(s);
        s.close();
    }
}
