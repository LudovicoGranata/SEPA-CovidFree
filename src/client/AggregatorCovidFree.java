package client;

import java.io.IOException;
import java.util.List;

import it.unibo.arces.wot.sepa.commons.exceptions.SEPABindingsException;
import it.unibo.arces.wot.sepa.commons.exceptions.SEPAPropertiesException;
import it.unibo.arces.wot.sepa.commons.exceptions.SEPAProtocolException;
import it.unibo.arces.wot.sepa.commons.exceptions.SEPASecurityException;
import it.unibo.arces.wot.sepa.commons.response.QueryResponse;
import it.unibo.arces.wot.sepa.commons.response.Response;
import it.unibo.arces.wot.sepa.commons.security.ClientSecurityManager;
import it.unibo.arces.wot.sepa.commons.sparql.Bindings;
import it.unibo.arces.wot.sepa.commons.sparql.BindingsResults;
import it.unibo.arces.wot.sepa.commons.sparql.RDFTermLiteral;
import it.unibo.arces.wot.sepa.commons.sparql.RDFTermURI;
import it.unibo.arces.wot.sepa.pattern.Aggregator;
import it.unibo.arces.wot.sepa.pattern.GenericClient;
import it.unibo.arces.wot.sepa.pattern.JSAP;
import it.unibo.arces.wot.sepa.pattern.Producer;

public class AggregatorCovidFree extends Aggregator {

	private List<Bindings> existingData = null;
	
	public AggregatorCovidFree(JSAP appProfile, String subscribeID, String updateID, ClientSecurityManager sm)
			throws SEPAProtocolException, SEPASecurityException {
		super(appProfile, subscribeID, updateID, sm);
	}

	@Override
	public void onFirstResults(BindingsResults results) {
		System.out.println("First results: " + results.size());
		this.manageData(results.getBindings());
	}

	@Override
	public void onAddedResults(BindingsResults results) {
		System.out.println("Added results: " + results.size());
		this.manageData(results.getBindings());
	}

	private int manageData(List<Bindings> data) {
		int counter = 0;
		for (Bindings binding : data) {

			String region = binding.getValue("region");
			String timestamp = binding.getValue("timestamp");
			String value = binding.getValue("value");
			
			boolean toUpdate = true; // Di default immaginiamo di dover fare l'update
			if(this.existingData != null) {
				// Possiamo controllare se l'observation sia già presente nel DataCube
				for(Bindings b : this.existingData) {
					if (b.getValue("region").equals(region) && b.getValue("timestamp").equals(timestamp)) {
						if(b.getValue("value").equals(value)) {
							// L'observation "binding" è già presente nel DataCube ed il valore è corretto
							// AZIONE: evitiamo di inserirla nuovamente
							toUpdate = false;
						}else {
							// L'observation "binding" è già presente nel DataCube MA IL VALORE RISULTA ERRATO!
							// AZIONE: dobbiamo correggere il valore dell'observation pre-esistente
							fixValue(region, timestamp, value, b.getValue("value"));
							toUpdate = false;
						}
						break;
					}
				}
			}
			counter++;
			if(!toUpdate) continue;
			try {
				this.setUpdateBindingValue("region", new RDFTermURI(region));
				this.setUpdateBindingValue("timestamp", new RDFTermLiteral(timestamp));
				this.setUpdateBindingValue("value", new RDFTermLiteral(value));
			} catch (SEPABindingsException e) {
				e.printStackTrace();
				continue;
			}

			try {
				update();
			} catch (SEPASecurityException | SEPAProtocolException | SEPAPropertiesException
					| SEPABindingsException e) {
				e.printStackTrace();
				continue;
			}
			System.out.println(counter);
		}
		System.out.println("Operation finished.");
		return counter;
	}

	private void fixValue(String region, String timestamp, String newValue, String oldValue) {
		Producer fixContext;
		String updateID = "FIX_EXISTING_VALUE";
		try {
			fixContext = new Producer(appProfile, updateID, null);
			fixContext.setUpdateBindingValue("region", new RDFTermURI(region));
			fixContext.setUpdateBindingValue("timestamp", new RDFTermLiteral(timestamp));
			fixContext.setUpdateBindingValue("newValue", new RDFTermLiteral(newValue));
			fixContext.setUpdateBindingValue("oldValue", new RDFTermLiteral(oldValue));
			fixContext.update();
			fixContext.close();
		} catch (SEPAProtocolException | SEPASecurityException | SEPAPropertiesException | SEPABindingsException
				| IOException e) {
			System.err.println("Something went wrong during execution of " + updateID);
		}
		System.out.println("FIXED existing value: " + region + " @ " + timestamp + " NEW: " + newValue + " OLD: " + oldValue);
	}

	public static void main(String[] args) throws SEPAProtocolException, SEPASecurityException, SEPAPropertiesException,
			SEPABindingsException, IOException

	{

		JSAP appProfile = new JSAP("resources/AggregatorCovidFree.jsap");

		AggregatorCovidFree app = new AggregatorCovidFree(appProfile, "GET_OBSERVATIONS", "INSERT_OBSERVATION", null);
//		app.exec("DELETE");
//		app.exec("DELETE_CONTEXT");
//		app.exec("CREATE_DATASET");
//		app.exec("CREATE_ORGANIZATION");
//		app.exec("CREATE_DSD");
//		app.exec("CREATE_MEASURE");
//		app.exec("CREATE_DIMENSION_REGION");
//		app.exec("CREATE_DIMENSION_TIMESTAMP");

		app.existingData = app.query("GET_EXISTING_OBSERVATIONS");
		
		app.subscribe(5000);

		synchronized (app) {
			try {
				app.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		app.close();
	}

	private void exec(String updateID) {
		Producer deleteContext;
		try {
			deleteContext = new Producer(appProfile, updateID, null);
			deleteContext.update();
			deleteContext.close();
		} catch (SEPAProtocolException | SEPASecurityException | SEPAPropertiesException | SEPABindingsException
				| IOException e) {
			System.err.println("Something went wrong during execution of " + updateID);
		}
	}
	
	private List<Bindings> query(String queryID) {
		GenericClient queryContext = null;
		List<Bindings> results = null;
		try {
			queryContext = new GenericClient(appProfile, null, null);
			Response r = queryContext.query(queryID, null, 5000);
			if (!r.isQueryResponse())
				System.exit(1);
			QueryResponse queryResponse = (QueryResponse) r;
			results = queryResponse.getBindingsResults().getBindings();
		} catch (SEPAProtocolException | SEPASecurityException | SEPAPropertiesException | SEPABindingsException e) {
			System.err.println("Something went wrong during execution of " + queryID);
		} finally {
			try {
				if(queryContext != null) queryContext.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return results;
	}

}
