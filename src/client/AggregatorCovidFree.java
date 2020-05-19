package client;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import it.unibo.arces.wot.sepa.commons.exceptions.SEPABindingsException;
import it.unibo.arces.wot.sepa.commons.exceptions.SEPAPropertiesException;
import it.unibo.arces.wot.sepa.commons.exceptions.SEPAProtocolException;
import it.unibo.arces.wot.sepa.commons.exceptions.SEPASecurityException;
import it.unibo.arces.wot.sepa.commons.security.ClientSecurityManager;
import it.unibo.arces.wot.sepa.commons.sparql.Bindings;
import it.unibo.arces.wot.sepa.commons.sparql.BindingsResults;
import it.unibo.arces.wot.sepa.commons.sparql.RDFTermLiteral;
import it.unibo.arces.wot.sepa.commons.sparql.RDFTermURI;
import it.unibo.arces.wot.sepa.pattern.Aggregator;
import it.unibo.arces.wot.sepa.pattern.JSAP;
import it.unibo.arces.wot.sepa.pattern.Producer;
import util.DateHelper;

public class AggregatorCovidFree extends Aggregator {

	private boolean firstResultsLoaded;
	private Instant today;

	public AggregatorCovidFree(JSAP appProfile, String subscribeID, String updateID, ClientSecurityManager sm)
			throws SEPAProtocolException, SEPASecurityException {
		super(appProfile, subscribeID, updateID, sm);
		this.firstResultsLoaded = false;
	}

	// viene aggiunto un risultato
	@Override
	public void onFirstResults(BindingsResults results) {
		System.out.println("First results: " + results.size());
		int counter = 0;
		for (Bindings binding : results.getBindings()) {

			String region = binding.getValue("region");
			String timestamp = binding.getValue("timestamp");
			String value = binding.getValue("value");

			if (this.firstResultsLoaded && DateHelper.toUTC(timestamp).isBefore(today)) {
				continue;
			}

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
			counter++;
			System.out.println(counter);
		}
		this.firstResultsLoaded = true;
		this.today = Instant.now().truncatedTo(ChronoUnit.DAYS);
	}

	@Override
	public void onAddedResults(BindingsResults results) {
		System.out.println("Added results: " + results.size());
		int counter = 0;
		for (Bindings binding : results.getBindings()) {
			String region = binding.getValue("region");
			String timestamp = binding.getValue("timestamp");
			String value = binding.getValue("value");
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
			counter++;
			System.out.println(counter);
		}
	}

	@Override
	public void onBrokenConnection() {
		this.today = Instant.now().truncatedTo(ChronoUnit.DAYS);
		if (this.firstResultsLoaded) {
			deleteToday();
		} else {
			this.exec("DELETE");
		}
		super.onBrokenConnection();
	}

	private void deleteToday() {
		try {
			Producer prod = new Producer(appProfile, "DELETE_SINCE", null);
			prod.setUpdateBindingValue("since", new RDFTermLiteral(today.toString()));
			prod.update();
			prod.close();
		} catch (SEPABindingsException | SEPAProtocolException | SEPASecurityException | SEPAPropertiesException
				| IOException e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) throws SEPAProtocolException, SEPASecurityException, SEPAPropertiesException,
			SEPABindingsException, IOException

	{

		JSAP appProfile = new JSAP("resources/AggregatorCovidFree.jsap");

		AggregatorCovidFree app = new AggregatorCovidFree(appProfile, "GET_OBSERVATIONS", "INSERT_OBSERVATION", null);
		app.exec("DELETE");
		app.exec("DELETE_CONTEXT");
		app.exec("CREATE_DATASET");
		app.exec("CREATE_ORGANIZATION");
		app.exec("CREATE_DSD");
		app.exec("CREATE_MEASURE");
		app.exec("CREATE_DIMENSION_REGION");
		app.exec("CREATE_DIMENSION_TIMESTAMP");
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
			e.printStackTrace();
		}

	}

}
