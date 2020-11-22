package sample.camel;

import org.apache.camel.Exchange;

public class AggregationStrategy implements org.apache.camel.AggregationStrategy {
    @Override
    public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
        if (oldExchange == null) {
            // we start a new correlation group, so complete all previous groups
            newExchange.setProperty(Exchange.AGGREGATION_COMPLETE_ALL_GROUPS, true);
            return newExchange;
        }

        String oldBody = oldExchange.getIn().getBody(String.class);
        String newBody = newExchange.getIn().getBody(String.class);
        oldExchange.getIn().setBody(oldBody + "," + newBody);
        return oldExchange;
    }
}
