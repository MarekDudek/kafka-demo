package md;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public enum PropertiesHelper
{
    ;

    public static Properties loadFromFile(final String name) throws IOException
    {
        final Properties p = new Properties();
        final FileInputStream s = new FileInputStream(name);
        p.load(s);
        s.close();
        return p;
    }
}
