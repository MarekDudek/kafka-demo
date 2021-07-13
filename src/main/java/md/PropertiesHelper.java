package md;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;

import static java.util.Optional.empty;
import static java.util.Optional.of;

public enum PropertiesHelper
{
    ;

    public static Optional<Properties> loadFromFile(final String name)
    {
        final Properties p = new Properties();
        try
        {
            final FileInputStream s = new FileInputStream(name);
            p.load(s);
            s.close();
            return of(p);
        }
        catch (final IOException e)
        {
            e.printStackTrace();
            return empty();
        }
    }
}
