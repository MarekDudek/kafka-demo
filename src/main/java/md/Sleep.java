package md;

import java.time.Duration;

public enum Sleep
{
    ;


    public static void sleep(final Duration duration)
    {
        try
        {
            Thread.sleep(duration.toMillis());
        }
        catch (final InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }
}
