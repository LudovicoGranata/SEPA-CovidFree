package client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import client.persistence.MySqlObservationDAO;
import client.persistence.ObservationDTO;
import it.unibo.arces.wot.sepa.commons.exceptions.SEPABindingsException;
import it.unibo.arces.wot.sepa.commons.exceptions.SEPAPropertiesException;
import it.unibo.arces.wot.sepa.commons.exceptions.SEPAProtocolException;
import it.unibo.arces.wot.sepa.commons.exceptions.SEPASecurityException;
import it.unibo.arces.wot.sepa.commons.security.ClientSecurityManager;
import it.unibo.arces.wot.sepa.commons.sparql.Bindings;
import it.unibo.arces.wot.sepa.commons.sparql.BindingsResults;
import it.unibo.arces.wot.sepa.pattern.Consumer;
import it.unibo.arces.wot.sepa.pattern.JSAP;
import util.DateHelper;

public class ConsumerCovidFree extends Consumer {

	private int obsId = 0;
	private MySqlObservationDAO dao = new MySqlObservationDAO();
	
	public ConsumerCovidFree(JSAP appProfile, String subscribeID, ClientSecurityManager sm)
			throws SEPAProtocolException, SEPASecurityException {
		super(appProfile, subscribeID, sm);
	}

	@Override
	public void onBrokenConnection() {
		System.out.println("BROKEN CONNECTION");
		this.init();
		super.onBrokenConnection();
	}

	@Override
	public void onAddedResults(BindingsResults results) {
		this.persist(results.getBindings());
	}
	
	@Override
	public void onRemovedResults(BindingsResults results) {
		this.remove(results.getBindings());
	}

	@Override
	public void onFirstResults(BindingsResults results) {
		this.persist(results.getBindings());
	}

	private void init() {
		dao.dropTable();
		dao.createTable();
	}
	
//	private void persist(List<Bindings> results) {
//		List<ObservationDTO> list = new ArrayList<>();
//		if(results.size() >= 1) {
//			for (Bindings result : results) {
//				ObservationDTO entry = new ObservationDTO();
//				entry.setId(++obsId);
//				entry.setRegion(result.getValue("region"));
//				entry.setTimestamp(DateHelper.toUTC(result.getValue("timestamp")));
//				entry.setValue(Integer.parseInt(result.getValue("value")));
//				list.add(entry);
//			}
//			dao.createAll(list);
//		}
//		System.out.println("Persisted " + list.size() + " observations.");
//	}
	
	private void persist(List<Bindings> results) {
		int count = 0;
		for (Bindings result : results) {
			ObservationDTO entry = new ObservationDTO();
			entry.setId(++obsId);
			entry.setRegion(result.getValue("region"));
			entry.setTimestamp(DateHelper.toUTC(result.getValue("timestamp")));
			entry.setValue(Integer.parseInt(result.getValue("value")));
			if (dao.create(entry)) {
				count++;
			} else {
				System.out.println("Failed: "+result.getValue("region")+" "+DateHelper.toUTC(result.getValue("timestamp")));
			}
		}
		System.out.println("Persisted " + count + " observations.");
	}
	
	private void remove(List<Bindings> results) {
		int count = 0;
		for (Bindings result : results) {
			ObservationDTO entry = new ObservationDTO();
			entry.setRegion(result.getValue("region"));
			entry.setTimestamp(DateHelper.toUTC(result.getValue("timestamp")));
			if (dao.deleteByRegionAndTimestamp(entry)) {
				count++;
			}
		}
		System.out.println("Removed " + count + " observations.");
	}
	
	public static void main(String[] args) {
		JSAP appProfile;
		ConsumerCovidFree app = null;
		try {
			appProfile = new JSAP("resources/AggregatorCovidFree.jsap");
			app = new ConsumerCovidFree(appProfile, "GET_DATACUBE_OBSERVATIONS", null);
			app.init();
			app.subscribe(5000);
		} catch (SEPAPropertiesException | SEPASecurityException | SEPAProtocolException | SEPABindingsException e1) {
			e1.printStackTrace();
		}

		synchronized (app) {
			try {
				app.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		try {
			app.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
