import com.zhunter.graph.GraphViz;

import java.io.File;

/**
 Graphviz gv = new Graphviz();                           //Graphviz Object.
 Graph graph = new Graph("g1", GraphType.DIGRAPH);       //Create New Gpaph.
 graph.addAttribute(new Attribute("rankdir","LR"));      //Add some attribute.
 Node n1 = new Node("N1");                               //Create Node Object.
 n1.addAttributesnew Attribute("label","\" Node1 \""));  //Add attribute
 Node n2 = new Node("N2");
 Node n3 = new Node("N3");

 graph.addNode(n1);                                      //Add node to graph.
 graph.addNode(n2);
 graph.addNode(n3);
 graph.addEdge(new Edge(n1, n2));                        //Add edge
 graph.addEdge(new Edge(n2, n3));
 graph.addEdge(new Edge(n3,n1));
 String type = "png";
 File out = new File(tmpPath+"/outEX1."+ type);          //Output File.
 this.writeGraphToFile( gv.getGraphByteArray(graph, type, "100"), out );
 * */
/**
 struct1 [label="{{{{<f#cnt#0> cnt#0}|{<fe#cnt#0> 1 AS cnt#0 UNION 2 AS ppp#2}}}|{<f0> tableName: join}|{<cnt#0> cnt#0}}"]
 struct4 [label="{{{{<f#ppp#2> ppp#2}|{<fe#ppp#2> 2 AS ppp#2}}}|{<f0> tableName: OneRowRelation}|{}}"]
 struct3 [label="{{{{<f#cnt#0> cnt#0}|{<fe#cnt#0> 1 AS cnt#0}}}|{<f0> tableName: OneRowRelation}|{}}"]
 struct0 [label="{{{{<f#cnt#5> cnt#5}|{<fe#cnt#5> sum(cast(cnt#0 as bigint)) AS cnt#5L}}}|{<f0> tableName: null}|{<cnt#0> cnt#0}}"]
 struct2 [label="{{{{<f#cnt#0> cnt#0}|{<fe#cnt#0> 1 AS cnt#0 UNION 2 AS ppp#2}}}|{<f0> tableName: SubqueryAlias.a}|{<cnt#0> cnt#0|<ppp#2> ppp#2}}"]
 struct0:"cnt#0" -> struct1:"f#cnt#0"
 struct1:"cnt#0" -> struct2:"f#cnt#0"
 struct2:"ppp#2" -> struct4:"f#ppp#2"
 struct2:"cnt#0" -> struct3:"f#cnt#0"
 * */
public class GraphvizJavaTest {
    public static void createDotGraph(String dotFormat,String fileName)
    {
        GraphViz gv=new GraphViz();
        gv.addln(gv.start_graph());
        gv.add(dotFormat);
        gv.addln(gv.end_graph());
        // String type = "gif";
        String type = "pdf";
        // gv.increaseDpi();
        gv.decreaseDpi();
        gv.decreaseDpi();
        File out = new File(fileName+"."+ type);
        gv.writeGraphToFile( gv.getGraph( gv.getDotSource(), type ), out);
    }
    public static void main(String[] args) throws Exception {
        String dotFormat="\tnode [shape=record]\n" +
                "\tstruct1 [label=\"{{{{<f#cnt#0> cnt#0}|{<fe#cnt#0> 1 AS cnt#0 UNION 2 AS ppp#2}}}|{<f0> tableName: join}|{<cnt#0> cnt#0}}\"]\n" +
                "\tstruct4 [label=\"{{{{<f#ppp#2> ppp#2}|{<fe#ppp#2> 2 AS ppp#2}}}|{<f0> tableName: OneRowRelation}|{}}\"]\n" +
                "\tstruct3 [label=\"{{{{<f#cnt#0> cnt#0}|{<fe#cnt#0> 1 AS cnt#0}}}|{<f0> tableName: OneRowRelation}|{}}\"]\n" +
                "\tstruct0 [label=\"{{{{<f#cnt#5> cnt#5}|{<fe#cnt#5> sum(cast(cnt#0 as bigint)) AS cnt#5L}}}|{<f0> tableName: null}|{<cnt#0> cnt#0}}\"]\n" +
                "\tstruct2 [label=\"{{{{<f#cnt#0> cnt#0}|{<fe#cnt#0> 1 AS cnt#0 UNION 2 AS ppp#2}}}|{<f0> tableName: SubqueryAlias.a}|{<cnt#0> cnt#0|<ppp#2> ppp#2}}\"]\n" +
                "\tstruct0:\"cnt#0\" -> struct1:\"f#cnt#0\"\n" +
                "\tstruct1:\"cnt#0\" -> struct2:\"f#cnt#0\"\n" +
                "\tstruct2:\"ppp#2\" -> struct4:\"f#ppp#2\"\n" +
                "\tstruct2:\"cnt#0\" -> struct3:\"f#cnt#0\"";
        createDotGraph(dotFormat, "DotGraph");
    }

}
