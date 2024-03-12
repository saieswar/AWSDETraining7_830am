Spark Optimization Plan:
--------------------------------------------------------------------------------------
When a query (through SQL/Dataframe) is submitted to spark, it goes through different logical plans before it is actually executed to get results.
EXPLAIN command can be used to print the various or all of the plans that executed during the course of execution.
1) explain() - which by default has value False, which means it will print only the physical plan.
2) explain(True) - it will print all the plans explained below:
3) explain(mode="XXX") : mode is also optional and can take below values:
 simple: Print only a physical plan.
 extended: Print both logical and physical plans.
 codegen: Print a physical plan and generated codes if they are available.
 cost: Print a logical plan and statistics if they are available.
 formatted: Split explain output into two sections: a physical plan outline and node details.

1) Parsed Logical plan (Unresolved):
 in this plan, any syntax errors will be caught
 
2) Analyzed Logical Plan: (resolved)
 in this plan , any errors in the names of the projection objects (columns/tables in select stmts) would be analyzed 
 
3) Optimized Logical Plan:
 there are certain set of pre-defined rules, based on which the plan will be optimized , like:
 i) predicate pushdown -> which pushing the filters down so that we filter the data as early as possible so we have less data for processing
 ii) combine multi projection into one (select stmts)
 iii) combine multi filters into one (where clauses)
 
 ** explain(true) will display details of all the plans executed when the query is submitted 
 == Parsed Logical Plan ==
 All synta errors are checked 
 == Analyzed Logical Plan ==
 shows all the resolved column names and table name (Relation is the output of this plan) 
 == Optimized Logical Plan ==
 
4) Physical Plan: 
 if we are using any aggregation/join in the query, it will decide 
 1) what aggregation strategy to be used (hash/sort aggregate)
 2) what join strategy to be used (broadcast hash join/sort merge join/shuffle hash join) 
There can be multiple Physical plans generated, but only the most cost effective one will be selected and code generated for the same (codeGen), 
which finally gets executed to get the result.