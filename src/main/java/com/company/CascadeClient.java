package com.company;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.operation.DebugLevel;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.expression.ExpressionFunction;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import java.util.Properties;


public class CascadeClient {

    public static void main(String[] args) {
        Scheme schIn2 = new TextDelimited(new Fields("id_2", "oddeven_2", "name"), ",");

        Tap srctap2 = new Hfs(schIn2, args[1]);
        Pipe lhs = new Pipe("lhs");

        Tap sinkTap = new Hfs(new TextDelimited(true, ","), args[2], SinkMode.REPLACE);


        lhs = new Each(lhs,new Fields("oddeven_2"),new ExpressionFilter("oddeven_2 != 1"));
        ExpressionFunction ef = new ExpressionFunction(new Fields("segement"),"id_2 - 10");
        FlowDef flowDef = FlowDef.flowDef().addSource(lhs,srctap2).addSink(lhs,sinkTap);
        flowDef.setDebugLevel( DebugLevel.VERBOSE );

        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties,CascadeClient.class);
        Hadoop2MR1FlowConnector flowConnector = new Hadoop2MR1FlowConnector(properties);

        Flow flow = flowConnector.connect(flowDef);
        flow.complete();



    }
}
