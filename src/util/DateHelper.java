package util;

import java.time.Instant;
import java.time.OffsetDateTime;

public class DateHelper {

	public static Instant fromSqlToJava(java.sql.Timestamp data) {
		return data.toInstant();
	}
	
	public static java.sql.Timestamp fromJavaToSql(Instant data) {
		return java.sql.Timestamp.from(data);
	}
	
	public static Instant toUTC(String zonedTimestamp) {
		OffsetDateTime odt = OffsetDateTime.parse(zonedTimestamp);
		return odt.toInstant();
	}
}
