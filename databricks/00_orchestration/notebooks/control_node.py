# Databricks notebook source
# MAGIC %md
# MAGIC `control` plan nodes. `mode=expression` applies an SSIS Expression Task assignment
# MAGIC (`@[User::ExtractAttempt] = @[User::ExtractAttempt] + 1`); `mode=query` runs a translated
# MAGIC single-value query into `resultVariable`; `mode=statement` runs a translated statement or the
# MAGIC `etl.usp_LogError` call it stood for. The assigned variable is published as a task value and the
# MAGIC node's outgoing expression edges are evaluated into `edge_<n>` task values.

# COMMAND ----------
# MAGIC %run ./_bootstrap

# COMMAND ----------
mode = widget("mode")
variables = readVariables()
resultVariable = widget("resultVariable", "")

if mode == "expression":
    name, value = plan_expression.assign(widget("assignment"), variables)
    setTaskValue(name.split("::", 1)[1], value)
    print({"assigned": name, "value": value})
elif mode in ("query", "statement"):
    statement = json.loads(widget("statement"))
    paramNames = json.loads(widget("statementParameters", "[]") or "[]")
    values = [variables[p] for p in paramNames]
    if "call" in statement:
        args = dict(statement["args"])
        if args.get("batchId") is None and "User::BatchId" in variables:
            args["batchId"] = variables["User::BatchId"]
        getattr_control = {"logError": control.logError}[statement["call"]]
        getattr_control(spark, ctx["catalog"], **args)
        print({"call": statement["call"], "args": args})
    else:
        sql = naming.translateLegacyReferences(ctx["catalog"], statement["sql"])
        sql, args = tsql_translate.bindParameters(sql, values)
        df = spark.sql(sql, args=args) if args else spark.sql(sql)
        if mode == "query":
            row = df.first()
            value = None if row is None else row[0]
            default = variables.get(resultVariable)
            if value is None:
                value = default
            elif isinstance(default, bool):
                value = bool(value)
            elif isinstance(default, int):
                value = int(value)
            variables[resultVariable] = value
            setTaskValue(resultVariable.split("::", 1)[1], value)
            print({"sql": sql, "result": value})
        else:
            print({"sql": sql, "affected": [r.asDict() for r in df.collect()]})
else:
    raise ValueError(f"unknown control mode '{mode}'")

print({"edges": evaluateEdges(variables)})
