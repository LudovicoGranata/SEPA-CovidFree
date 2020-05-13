package aggregator;

import java.time.Duration;
import java.time.Instant;

public class Cronometro {
	private static Instant start;

	public static void start() {
		start = Instant.now();
	}
	
	public static void stop(String name) {
		Instant stop = Instant.now();
		System.out.println(name + " took: " + Duration.between(start, stop));
	}
}
