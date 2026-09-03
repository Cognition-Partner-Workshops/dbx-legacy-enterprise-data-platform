/*
    OLTP estate schemas

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 00_schemas / 0000 - first script of the OLTP extension
    Depends on    : the shipped WideWorldImporters database (Application, Sales,
                    Warehouse, Purchasing, Sequences schemas already exist)
    Called by     : deployment scripts, run once per environment

    The trading application grew outwards from the original four schemas. Each
    of the schemas below was added by a different project team in a different
    year, which is why their conventions are not entirely consistent with each
    other (Shipping uses "Header/Line", Returns uses "Authorization/Line",
    Ecommerce uses no suffix at all). That inconsistency is deliberate and is
    preserved here.
*/
IF SCHEMA_ID(N'Shipping') IS NULL
    EXEC (N'CREATE SCHEMA [Shipping] AUTHORIZATION [dbo];');
GO

IF SCHEMA_ID(N'Returns') IS NULL
    EXEC (N'CREATE SCHEMA [Returns] AUTHORIZATION [dbo];');
GO

IF SCHEMA_ID(N'Loyalty') IS NULL
    EXEC (N'CREATE SCHEMA [Loyalty] AUTHORIZATION [dbo];');
GO

IF SCHEMA_ID(N'Ecommerce') IS NULL
    EXEC (N'CREATE SCHEMA [Ecommerce] AUTHORIZATION [dbo];');
GO

/*
    Integration already exists in the shipped sample (it holds the DW feed
    procedures). The estate reuses it for interface queues rather than adding
    yet another schema, which is what the original integration team did.
*/
IF SCHEMA_ID(N'Integration') IS NULL
    EXEC (N'CREATE SCHEMA [Integration] AUTHORIZATION [dbo];');
GO
