package client.persistence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import util.DateHelper;

public class MySqlObservationDAO {
	public static final String TABLE = "contagiGiornalieri";
	private static final String ID = "id";
	private static final String REGION = "region";
	private static final String TIMESTAMP = "timestamp";
	private static final String VALUE = "value";
	
	public boolean create(ObservationDTO observation) {
		boolean outcome = false;
		if ( observation == null )  {
			System.out.println( "create(): failed to insert a null entry");
			return outcome;
		}
		Connection conn = ConnectionFactory.createConnection();
		try {
			String insert = 
					"INSERT " +
						"INTO " + TABLE + " ( " + 
							ID +", " + REGION + ", " + TIMESTAMP + ", " + VALUE + ") " +
						"VALUES (?,?,?,?)";
			PreparedStatement prep_stmt = conn.prepareStatement(insert);
			prep_stmt.clearParameters();
			prep_stmt.setInt(1, observation.getId());
			prep_stmt.setString(2, observation.getRegion());
			prep_stmt.setTimestamp(3, DateHelper.fromJavaToSql(observation.getTimestamp()));
			prep_stmt.setInt(4, observation.getValue());
			prep_stmt.executeUpdate();
			outcome = true;
			prep_stmt.close();
		}
		catch (Exception e) {
			// e.printStackTrace();
		}
		
		return outcome;
	}
	
	public boolean createAll(Collection<ObservationDTO> observations) {
		boolean outcome = false;
		if ( observations == null || observations.size() < 1)  {
			System.out.println( "create(): failed to insert a null collection of entries");
			return outcome;
		}
		Connection conn = ConnectionFactory.createConnection();
		try {
			String insert = 
					"INSERT " +
						"INTO " + TABLE + " ( " + 
							ID +", " + REGION + ", " + TIMESTAMP + ", " + VALUE + ") " +
						"VALUES (?,?,?,?)";
			for (int i = 1; i < observations.size(); i++) {
				insert += ",(?,?,?,?)";
			}
			PreparedStatement prep_stmt = conn.prepareStatement(insert);
			prep_stmt.clearParameters();
			int i = 0;
			for (ObservationDTO observation : observations) {
				int index = i*4;
				prep_stmt.setInt(++index, observation.getId());
				prep_stmt.setString(++index, observation.getRegion());
				prep_stmt.setTimestamp(++index, DateHelper.fromJavaToSql(observation.getTimestamp()));
				prep_stmt.setInt(++index, observation.getValue());
				i++;
			}
			
			prep_stmt.executeUpdate();
			outcome = true;
			prep_stmt.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return outcome;
	}

	public ObservationDTO readById(int id) {
		ObservationDTO observation = null;
		
		Connection conn = ConnectionFactory.createConnection();
		try {
			String read_by_id = 
					"SELECT * " +
						"FROM " + TABLE + " " +
						"WHERE " + ID + " = ? ";
			PreparedStatement prep_stmt = conn.prepareStatement(read_by_id);
			prep_stmt.clearParameters();
			prep_stmt.setInt(1, id);
			ResultSet rs = prep_stmt.executeQuery();
			if ( rs.next() ) {
				ObservationDTO entry = new ObservationDTO();
				entry.setId(rs.getInt(ID));
				entry.setRegion(rs.getString(REGION));
				entry.setTimestamp(DateHelper.fromSqlToJava(rs.getTimestamp(TIMESTAMP)));
				entry.setValue(rs.getInt(VALUE));
				observation = entry;
			}
			rs.close();
			prep_stmt.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return observation;
	}

	public List<ObservationDTO> readAll() {
		List<ObservationDTO> observations = new ArrayList<ObservationDTO>();
		
		Connection conn = ConnectionFactory.createConnection();
		try {
			String read_all = 
					"SELECT * " +
						"FROM " + TABLE ;
			Statement prep_stmt = conn.createStatement();
			ResultSet rs = prep_stmt.executeQuery(read_all);
			while ( rs.next() ) {
				ObservationDTO entry = new ObservationDTO();
				entry.setId(rs.getInt(ID));
				entry.setRegion(rs.getString(REGION));
				entry.setTimestamp(DateHelper.fromSqlToJava(rs.getTimestamp(TIMESTAMP)));
				entry.setValue(rs.getInt(VALUE));
				observations.add(entry);
			}
			rs.close();
			prep_stmt.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return observations;
	}

	public boolean update(ObservationDTO observation) {
		boolean outcome = false;
		if ( observation == null )  {
			System.out.println( "update(): failed to update a null entry");
			return outcome;
		}
		Connection conn = ConnectionFactory.createConnection();
		try {
			String update = 
					"UPDATE " + TABLE + " " +
						"SET " +
							REGION + " = ?, " +
							TIMESTAMP + " = ?, " +
							VALUE + " = ? " +
						"WHERE " + ID + " = ? ";
			PreparedStatement prep_stmt = conn.prepareStatement(update);
			prep_stmt.clearParameters();
			prep_stmt.setString(1, observation.getRegion());
			prep_stmt.setTimestamp(2, DateHelper.fromJavaToSql(observation.getTimestamp()));
			prep_stmt.setInt(3, observation.getValue());
			prep_stmt.setInt(4, observation.getId());
			prep_stmt.executeUpdate();
			outcome = true;
			prep_stmt.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return outcome;
	}

	public boolean delete(ObservationDTO observation) {
		boolean outcome = false;
		if ( observation == null )  {
			System.out.println("delete(): cannot delete a null entry");
			return outcome;
		}
		Connection conn = ConnectionFactory.createConnection();
		try {
			String delete = 
					"DELETE " +
						"FROM " + TABLE + " " +
						"WHERE " + ID + " = ? ";
			PreparedStatement prep_stmt = conn.prepareStatement(delete);
			prep_stmt.clearParameters();
			prep_stmt.setInt(1, observation.getId());
			prep_stmt.executeUpdate();
			outcome = true;
			prep_stmt.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return outcome;
	}
	
	public boolean deleteByRegionAndTimestamp(ObservationDTO observation) {
		boolean outcome = false;
		if ( observation == null )  {
			System.out.println("delete(): cannot delete a null entry");
			return outcome;
		}
		Connection conn = ConnectionFactory.createConnection();
		try {
			String delete = 
					"DELETE " +
						"FROM " + TABLE + " " +
						"WHERE " + REGION + " = ? AND " + TIMESTAMP + " = ?";
			PreparedStatement prep_stmt = conn.prepareStatement(delete);
			prep_stmt.clearParameters();
			prep_stmt.setString(1, observation.getRegion());
			prep_stmt.setTimestamp(2, DateHelper.fromJavaToSql(observation.getTimestamp()));
			prep_stmt.executeUpdate();
			outcome = true;
			prep_stmt.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return outcome;
	}

	public boolean createTable() {
		boolean outcome = false;
		Connection conn = ConnectionFactory.createConnection();
		try {
			Statement stmt = conn.createStatement();
			String create = 
					"CREATE " +
						"TABLE " + TABLE + " ( " +
							ID + " INT NOT NULL PRIMARY KEY, " +
							REGION + " VARCHAR(255) NOT NULL, " +
							TIMESTAMP + " DATETIME NOT NULL, " +
							VALUE + " INT NOT NULL, " +
							"UNIQUE (" + REGION + ", " + TIMESTAMP + ") "
						+ ")";
			System.out.println(create);
			stmt.execute(create);
			outcome = true;
			stmt.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return outcome;
	}

	public boolean dropTable() {
		boolean outcome = false;
		Connection conn = ConnectionFactory.createConnection();
		try {
			Statement stmt = conn.createStatement();
			String drop = 
					"DROP " +
						"TABLE " + TABLE;
			System.out.println(drop);
			stmt.execute(drop);
			outcome = true;
			stmt.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return outcome;
	}

}
