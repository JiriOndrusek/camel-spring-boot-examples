package sample.camel;


import org.apache.camel.Exchange;
import org.apache.camel.Processor;

import java.time.LocalDate;

public class CsvProcessor implements Processor {

    //symbol,exchange,name,dayCode,serverTimestamp,mode,lastPrice,tradeTimestamp,netChange,percentChange,unitCode,open,high,low,close,flag,volume
    //"IBM","NYSE","International Business Machines","J","2020-11-22T08:37:37-06:00","d","116.94","2020-11-20T19:00:00-05:00","-0.24000000000001","-0.2","2","117.6","118.04","116.69","116.94","s","5024593"
    //"AAPL","NASDAQ","Apple Inc","J","2020-11-22T08:37:37-06:00","d","117.34","2020-11-20T18:55:00-05:00","-1.3","-1.1","2","118.64","118.77","117.29","117.34","s","73604200"

    //symbol,exchange,name,dayCode,serverTimestamp,mode,lastPrice,tradeTimestamp,netChange,percentChange,unitCode,open,high,low,close,flag,volume
    //"COST","NASDAQ","Costco Wholesale","J","2020-11-22T09:10:29-06:00","d","381.12","2020-11-20T18:55:00-05:00","-2.42","-0.63","2","383.95","385.5","381.01","381.12","s","1809400"
    //"CRM","NYSE","Salesforce.com Inc","J","2020-11-22T09:10:29-06:00","d","258.04","2020-11-20T19:00:00-05:00","-6.61","-2.5","2","263.59","265.02","257.82","258.04","s","7289605"
    //"CSCO","NASDAQ","Cisco Systems Inc","J","2020-11-22T09:10:29-06:00","d","40.97","2020-11-20T18:55:00-05:00","-0.2","-0.49","2","41.05","41.39","40.92","40.97","s","19778900"
    //"CVS","NYSE","CVS Corp","J","2020-11-22T09:10:29-06:00","d","66.39","2020-11-20T19:00:00-05:00","0.33","0.5","2","65.9","66.87","65.82","66.39","s","8825118"
    //"CVX","NYSE","Chevron Corp","J","2020-11-22T09:10:29-06:00","d","85.79","2020-11-20T19:00:00-05:00","0.060000000000002","0.07","2","85.72","86.31","84.88","85.79","s","8296993"
    //"DD","NYSE","Du Pont De.Nemours Inc","J","2020-11-22T09:10:29-06:00","d","62.55","2020-11-20T19:00:00-05:00","0.5","0.81","2","61.9","62.75","61.73","62.55","s","2721720"
    //"DHR","NYSE","Danaher Corp","J","2020-11-22T09:10:29-06:00","d","225.76","2020-11-20T19:00:00-05:00","-0.070000000000022","-0.03","2","225.67","227.54","224.12","225.76","s","2353100"
    //"DIS","NYSE","Walt Disney Company","J","2020-11-22T09:10:29-06:00","d","141.07","2020-11-20T19:00:00-05:00","-0.65000000000001","-0.46","2","141.71","142.61","140.86","141.07","s","8485920"
    //"DOW","NYSE","Dow Inc","J","2020-11-22T09:10:29-06:00","d","54.73","2020-11-20T19:00:00-05:00","-0.61000000000001","-1.1","2","55.07","55.42","54.28","54.73","s","2871818"
    //"DUK","NYSE","Duke Energy Corp","J","2020-11-22T09:10:29-06:00","d","92.58","2020-11-20T19:00:00-05:00","0.88","0.96","2","91.7","92.68","91.38","92.58","s","4213890"
    //"EMR","NYSE","Emerson Electric Company","J","2020-11-22T09:10:29-06:00","d","75.96","2020-11-20T19:00:00-05:00","-1.28","-1.66","2","77","77.05","75.47","75.96","s","1810734"
    //"EXC","NASDAQ","Exelon Corp","J","2020-11-22T09:10:29-06:00","d","41.87","2020-11-20T18:55:00-05:00","-0.48","-1.13","2","42.24","42.56","41.78","41.87","s","4069900"
    //"F","NYSE","Ford Motor Company","J","2020-11-22T09:10:29-06:00","d","8.74","2020-11-20T19:00:00-05:00","-0.08","-0.91","2","8.81","8.82","8.68","8.74","s","57024269"
    //"FB","NASDAQ","Facebook Inc","J","2020-11-22T09:10:29-06:00","d","269.7","2020-11-20T18:55:00-05:00","-3.24","-1.19","2","272.56","273","269.41","269.7","s","18122400"
    //"FDX","NYSE","Fedex Corp","J","2020-11-22T09:10:29-06:00","d","276.69","2020-11-20T19:00:00-05:00","-3.16","-1.13","2","280.97","281.66","276.69","276.69","s","1887673"
    //"GD","NYSE","General Dynamics Corp","J","2020-11-22T09:10:29-06:00","d","148.36","2020-11-20T19:00:00-05:00","-1.63","-1.09","2","150","150.33","147.5","148.36","s","1358866"
    //"GE","NYSE","General Electric Company","J","2020-11-22T09:10:29-06:00","d","9.76","2020-11-20T19:00:00-05:00","0.1","1.04","2","9.64","9.83","9.59","9.76","s","80135495"
    //"GILD","NASDAQ","Gilead Sciences Inc","J","2020-11-22T09:10:29-06:00","d","60.11","2020-11-20T18:55:00-05:00","-0.52","-0.86","2","59.6","60.26","59.48","60.11","s","9445000"
    //"GM","NYSE","General Motors Company","J","2020-11-22T09:10:29-06:00","d","43.04","2020-11-20T19:00:00-05:00","0.22","0.51","2","42.81","43.32","42.37","43.04","s","16887844"
    //"GOOG","NASDAQ","Alphabet Cl C","J","2020-11-22T09:10:29-06:00","d","1742.19","2020-11-20T18:55:00-05:00","-21.73","-1.23","2","1765.21","1774","1741.86","1742.19","s","2313600"
    //"GOOGL","NASDAQ","Alphabet Cl A","J","2020-11-22T09:10:29-06:00","d","1736.38","2020-11-20T18:55:00-05:00","-22.19","-1.26","2","1762","1768.36","1735","1736.38","s","1385300"
    //"GS","NYSE","Goldman Sachs Group","J","2020-11-22T09:10:29-06:00","d","223.35","2020-11-20T19:00:00-05:00","-1.22","-0.54","2","223","224.81","222.63","223.35","s","1848687"
    //"HD","NYSE","Home Depot","J","2020-11-22T09:10:29-06:00","d","269.81","2020-11-20T19:00:00-05:00","-1.12","-0.41","2","271.45","272.37","268.87","269.81","s","4112543"
    //"HON","NYSE","Honeywell International Inc","J","2020-11-22T09:10:29-06:00","d","202","2020-11-20T19:00:00-05:00","-0.72","-0.36","2","201.77","203.33","200.5","202","s","2359051"
    //"IBM","NYSE","International Business Machines","J","2020-11-22T09:10:29-06:00","d","116.94","2020-11-20T19:00:00-05:00","-0.24000000000001","-0.2","2","117.6","118.04","116.69","116.94","s","5024593"
    //amibroker import: ticker, ymd, open, high, low, clods,volume

    private static final String TIMESTAMP = "serverTimestamp";
    private static final String[] COLUMNS = new String[] {"symbol", TIMESTAMP, "open", "high", "low", "close", "volume"};



    public void process(Exchange exchange) throws Exception {
        Object[][] custom = exchange.getIn().getBody(Object[][].class);

        String timestamp = null;
        int timestampIndex = 0;

        //get indexes for results
        int[] indexes = new int[COLUMNS.length];

        for(int i = 0; i < custom[0].length; i++) {
            for(int j = 0; j < COLUMNS.length; j++) {
                if(TIMESTAMP.equals(custom[0][i])) {
                    timestamp = ((String)custom[1][i]);
                    timestamp = timestamp.substring(0, timestamp.indexOf("T"));
                }
                if(COLUMNS[j].equals(TIMESTAMP)) {
                    timestampIndex = j;
                }
                if(COLUMNS[j].equals(custom[0][i])) {
                    indexes[j] = i;
                    break;
                }
            }
        }

        //create new data
        String[][] data = new String[custom.length-1][COLUMNS.length];
        for(int i = 0; i < data.length; i++) {
            for(int j = 0; j < indexes.length; j++) {
                if(j == timestampIndex) {
                    data[i][j] = timestamp;
                } else {
                    data[i][j] = (String) custom[i + 1][indexes[j]];
                }
            }
        }
        exchange.getMessage().setBody(data);
        exchange.getMessage().setHeader("timestamp", timestamp);

        System.out.println(data);
    }
}
