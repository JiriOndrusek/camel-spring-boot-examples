/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sample.camel;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.dataformat.csv.CsvDataFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * A simple Camel route that triggers from a timer and calls a bean and prints to system out.
 * <p/>
 * Use <tt>@Component</tt> to make Camel auto detect this route when starting.
 */
@Component
public class MyCamelRouter extends RouteBuilder {

    @Override
    public void configure() throws Exception {
        CsvDataFormat csv = new CsvDataFormat();
        csv.setDelimiter(',');



        from("timer://foo?repeatCount=1")
                .setBody(constant("{{symbols}}"))
                .split(body().tokenize(","))
        .to("direct:start");

         from("direct:start")

                 .aggregate(constant("id"), new AggregationStrategy())
                    .completionSize(25).completionTimeout(1000)
                 .setHeader("CamelHttpMethod", constant("GET"))
                 .toD("http://marketdata.websol.barchart.com/getQuote.csv?apikey={{apiKey}}&symbols=${body}")
//                 .setBody(constant("symbol,exchange,name,dayCode,serverTimestamp,mode,lastPrice,tradeTimestamp,netChange,percentChange,unitCode,open,high,low,close,flag,volume\n" +
//                         "\"IBM\",\"NYSE\",\"International Business Machines\",\"J\",\"2020-11-22T08:37:37-06:00\",\"d\",\"116.94\",\"2020-11-20T19:00:00-05:00\",\"-0.24000000000001\",\"-0.2\",\"2\",\"117.6\",\"118.04\",\"116.69\",\"116.94\",\"s\",\"5024593\"\n" +
//                         "\"AAPL\",\"NASDAQ\",\"Apple Inc\",\"J\",\"2020-11-22T08:37:37-06:00\",\"d\",\"117.34\",\"2020-11-20T18:55:00-05:00\",\"-1.3\",\"-1.1\",\"2\",\"118.64\",\"118.77\",\"117.29\",\"117.34\",\"s\",\"73604200\""))
                    .unmarshal(csv)
                    .process(new CsvProcessor())
                    .marshal(csv)
                    .toD("file:report/${header.timestamp}?fileName=report.csv&fileExist=append");
    }

}
