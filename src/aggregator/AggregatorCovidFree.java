package aggregator;

import java.io.IOException;

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

public class AggregatorCovidFree extends Aggregator {

	public AggregatorCovidFree(JSAP appProfile, String subscribeID, String updateID, ClientSecurityManager sm)
			throws SEPAProtocolException, SEPASecurityException {
		super(appProfile, subscribeID, updateID, sm);
	}

	// viene aggiunto un risultato
	@Override
	public void onFirstResults(BindingsResults results) {
		aggregate(results);
	}

	@Override
	public void onAddedResults(BindingsResults results) {
		aggregate(results);
	}

	private void aggregate(BindingsResults results) {
		System.out.println("New results: " + results.size());
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
		}
	}

	@Override
	public void onBrokenConnection() {
		System.exit(1);
	}
	
	public static void main(String[] args) throws SEPAProtocolException, SEPASecurityException, SEPAPropertiesException,
			SEPABindingsException, IOException

	{

		JSAP appProfile = new JSAP("resources/AggregatorCovidFree.jsap");
		
		Producer delete = new Producer(appProfile, "DELETE", null);
		delete.update();
		delete.close();
		Producer deleteContext = new Producer(appProfile, "DELETE_CONTEXT", null);
		deleteContext.update();
		deleteContext.close();
		
		AggregatorCovidFree app = new AggregatorCovidFree(appProfile, "GET_OBSERVATIONS", "INSERT_OBSERVATION", null);
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

}
