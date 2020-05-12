package aggregator;

import java.io.IOException;

import it.unibo.arces.wot.sepa.commons.exceptions.SEPABindingsException;
import it.unibo.arces.wot.sepa.commons.exceptions.SEPAPropertiesException;
import it.unibo.arces.wot.sepa.commons.exceptions.SEPAProtocolException;
import it.unibo.arces.wot.sepa.commons.exceptions.SEPASecurityException;
import it.unibo.arces.wot.sepa.commons.security.ClientSecurityManager;
import it.unibo.arces.wot.sepa.commons.sparql.Bindings;
import it.unibo.arces.wot.sepa.commons.sparql.BindingsResults;
import it.unibo.arces.wot.sepa.commons.sparql.RDFTerm;
import it.unibo.arces.wot.sepa.commons.sparql.RDFTermLiteral;
import it.unibo.arces.wot.sepa.commons.sparql.RDFTermURI;
import it.unibo.arces.wot.sepa.pattern.Aggregator;
import it.unibo.arces.wot.sepa.pattern.JSAP;

public class AggregatorCovidFree extends Aggregator {

	public AggregatorCovidFree(JSAP appProfile, String subscribeID, String updateID, ClientSecurityManager sm)
			throws SEPAProtocolException, SEPASecurityException {
		super(appProfile, subscribeID, updateID, sm);
		// TODO Auto-generated constructor stub
	}

	private long sum = 0;

	
	//viene aggiunto un risultato
	public void onAddedResults(BindingsResults results) {

		for (Bindings binding : results.getBindings()) {
			
			String value=null;
			String regione=null;
			
			//da cambiare
			String timestamp=null;
			
			value=binding.getValue("value");
			regione=binding.getValue("region");
			
			//da cambiare
			timestamp=binding.getValue("timestamp");
			
			try {
				this.setUpdateBindingValue("region", new RDFTermURI(regione) );
				this.setUpdateBindingValue("value", new RDFTermLiteral(value) );
				this.setUpdateBindingValue("timestamp", new RDFTermLiteral(timestamp) );
				
			} catch (SEPABindingsException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			try {
				update();
			} catch (SEPASecurityException | SEPAProtocolException | SEPAPropertiesException
					| SEPABindingsException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			

		}
	}

	public static void main(String[] args) throws SEPAProtocolException, SEPASecurityException, SEPAPropertiesException,
			SEPABindingsException, IOException

	{

		AggregatorCovidFree app = new AggregatorCovidFree(new JSAP("AggregatorCovidFree.jsap"), "GET_OBSERVATIONS", "INSERT_OBSERVATION", null);

		app.subscribe(5000);

		synchronized (app) {

			try {

				app.wait();

			} catch (InterruptedException e) {

			}

		}

		app.close();

	}

}
