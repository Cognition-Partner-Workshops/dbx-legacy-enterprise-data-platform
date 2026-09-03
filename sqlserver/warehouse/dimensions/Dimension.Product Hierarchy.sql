/*
    Object        : [Dimension].[Product Hierarchy]  (recursive, ragged parent-child hierarchy)
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Product Category.sql
    Depends on    : Sequences.ProductHierarchyKey
    Called by     : Integration.usp_LoadProductHierarchy

    Parent-child structure of arbitrary depth. It is ragged: the Ambient Grocery
    branch runs department -> class -> subclass -> category -> SKU group (five
    levels) while Novelty Items runs department -> category (two), because the
    2003 buying team never subdivided it and nobody has since.

    Three access patterns are materialised by the load procedure, all from the
    same parent-child edges:

      1. [Parent Product Hierarchy Key]  - the true edge, for recursive CTEs.
      2. [Hierarchy Path] / [Level]      - a materialised path for prefix search.
      3. [Level 1 Code] .. [Level 6 Code]- a flattened form for the 2009 cube,
                                           with short branches repeating their
                                           leaf value up to level 6 so the cube
                                           does not show blanks (the classic
                                           ragged-hierarchy fudge).
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Product Hierarchy', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Product Hierarchy]
    (
        [Product Hierarchy Key]         INT             CONSTRAINT [DF_Dimension_Product_Hierarchy_Key] DEFAULT (NEXT VALUE FOR [Sequences].[ProductHierarchyKey]) NOT NULL,
        [Hierarchy Node Code]           NVARCHAR(30)    NOT NULL,
        [Hierarchy Node Name]           NVARCHAR(100)   NOT NULL,
        [Parent Product Hierarchy Key]  INT             NULL,
        [Parent Node Code]              NVARCHAR(30)    NULL,
        [Node Type Code]                NVARCHAR(15)    NULL,   -- DEPT / CLASS / SUBCLASS / CATEGORY / SKUGROUP
        [Hierarchy Level]               SMALLINT        NOT NULL,
        [Hierarchy Path]                NVARCHAR(400)   NULL,
        [Hierarchy Sort Order]          NVARCHAR(400)   NULL,
        [Is Leaf Node]                  BIT             NULL,
        [Is Ragged Branch]              BIT             NULL,   -- branch shorter than the deepest branch
        [Leaf Descendant Count]         INT             NULL,

        [Level 1 Code]                  NVARCHAR(30)    NULL,
        [Level 1 Name]                  NVARCHAR(100)   NULL,
        [Level 2 Code]                  NVARCHAR(30)    NULL,
        [Level 2 Name]                  NVARCHAR(100)   NULL,
        [Level 3 Code]                  NVARCHAR(30)    NULL,
        [Level 3 Name]                  NVARCHAR(100)   NULL,
        [Level 4 Code]                  NVARCHAR(30)    NULL,
        [Level 4 Name]                  NVARCHAR(100)   NULL,
        [Level 5 Code]                  NVARCHAR(30)    NULL,
        [Level 5 Name]                  NVARCHAR(100)   NULL,
        [Level 6 Code]                  NVARCHAR(30)    NULL,
        [Level 6 Name]                  NVARCHAR(100)   NULL,

        [Product Category Key]          INT             NULL,
        [Owning Region Code]            NVARCHAR(10)    NULL,   -- GLOBAL where the node is common to all three
        [Is Regional Extension]         BIT             NULL,   -- APAC added two marketplace-only branches in 2015
        [Source System Code]            NVARCHAR(20)    NULL,
        [Is Active]                     BIT             NULL,
        [Valid From]                    DATETIME2(7)    NOT NULL,
        [Valid To]                      DATETIME2(7)    NOT NULL,
        [Lineage Key]                   INT             NOT NULL,
        [Last Load Batch Id]            BIGINT          NULL,
        CONSTRAINT [PK_Dimension_Product_Hierarchy] PRIMARY KEY CLUSTERED ([Product Hierarchy Key] ASC),
        CONSTRAINT [UQ_Dimension_Product_Hierarchy_Node] UNIQUE ([Hierarchy Node Code]),
        CONSTRAINT [CK_Dimension_Product_Hierarchy_Level] CHECK ([Hierarchy Level] BETWEEN 1 AND 6)
    );

    CREATE NONCLUSTERED INDEX [IX_Dimension_Product_Hierarchy_Parent]
        ON [Dimension].[Product Hierarchy] ([Parent Product Hierarchy Key] ASC)
        INCLUDE ([Hierarchy Node Code], [Hierarchy Level], [Is Leaf Node]);

    CREATE NONCLUSTERED INDEX [IX_Dimension_Product_Hierarchy_Path]
        ON [Dimension].[Product Hierarchy] ([Hierarchy Path] ASC);
END;
GO

/*
    Self-referencing edge. Added after the table so the constraint can be created
    on an empty table without ordering the seed rows; the load procedure disables
    it while it rebuilds the tree and re-enables it with CHECK afterwards.
*/
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'FK_Dimension_Product_Hierarchy_Parent')
    ALTER TABLE [Dimension].[Product Hierarchy] WITH NOCHECK
        ADD CONSTRAINT [FK_Dimension_Product_Hierarchy_Parent]
        FOREIGN KEY ([Parent Product Hierarchy Key])
        REFERENCES [Dimension].[Product Hierarchy] ([Product Hierarchy Key]);
GO
