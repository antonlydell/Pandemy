-- SQLite

-- The Database describes General Stores in Runscape and their trading history.
-- The primary keys are integers and will autoincrement.

-- General Stores in Runescape
CREATE TABLE IF NOT EXISTS Store (
    StoreId   INTEGER,
    StoreName TEXT  NOT NULL,
    Location  TEXT  NOT NULL,
    OwnerId   TEXT  NOT NULL,

    CONSTRAINT StorePk PRIMARY KEY (StoreId)
);

-- Owners of General Stores
CREATE TABLE IF NOT EXISTS Owner (
    OwnerId   INTEGER, 
    OwnerName TEXT NOT NULL,
    BirthDate TEXT, -- YYYY-MM-DD

    CONSTRAINT OwnerPk PRIMARY KEY (OwnerId)
);

-- Customers that have traded in a General Store
CREATE TABLE IF NOT EXISTS Customer (
    CustomerId         TEXT, 
    CustomerName       TEXT    NOT NULL,
    BirthDate          TEXT,
    Residence          TEXT,
    IsAdventurer       INTEGER NOT NULL, -- 1 if Adventurer and 0 if NPC 

    CONSTRAINT CustomerPk PRIMARY KEY (CustomerId),

    CONSTRAINT IsAdventurerBool CHECK (IsAdventurer IN (0, 1))
);

-- The available items in General Stores
CREATE TABLE IF NOT EXISTS Item (
    ItemId      INTEGER,
    ItemName    TEXT    NOT NULL,
    TaxId       INTEGER NOT NULL,
    YearAdded   INTEGER, -- The year the item was added to Runescape
    Description TEXT,

    CONSTRAINT ItemPk PRIMARY KEY (ItemId)
);

-- Tax rates of items
CREATE TABLE IF NOT EXISTS Tax (
    TaxId       INTEGER,
    TaxName     TEXT NOT NULL,
    Percentage  REAL NOT NULL,
    Description TEXT,

    CONSTRAINT TaxPk PRIMARY KEY (TaxId)
);

-- The supply of items in a General Store
CREATE TABLE IF NOT EXISTS StoreSupply (
    StoreSupplyId INTEGER, 
    StoreId       INTEGER NOT NULL,
    ItemId        TEXT NOT NULL,
    Quantity      INTEGER NOT NULL,
    Price         INTEGER NOT NULL,

    CONSTRAINT StoreSupplyPk PRIMARY KEY (StoreSupplyId),

    CONSTRAINT StoreIdFk FOREIGN KEY (StoreId)
        REFERENCES Item (ItemId),

    CONSTRAINT StoreSupplyItemIdFk FOREIGN KEY (ItemId)
        REFERENCES Item (ItemId)
);

-- Items bought by customer and sold by the store to the customer
CREATE TABLE IF NOT EXISTS ItemTradedInStore (
    TransactionId            INTEGER, 
    StoreId                  INTEGER NOT NULL,
    ItemId                   INTEGER NOT NULL,
    CustomerId,              INTEGER NOT NULL,
    CustomerBuys             INTEGER NOT NULL, -- 1 if the customer buys the item and 0 if he sells
    TransactionTimestamp     TEXT    NOT NULL, -- YYYY-MM-DD HH:MM:SS
    Quantity                 INTEGER NOT NULL,
    TradePricePerItem        REAL    NOT NULL,
    TradePricePerItemWithTax REAL    NOT NULL,
    TotalTradePrice          REAL    NOT NULL,
    TotalTradePriceWithTax   REAL    NOT NULL,

    CONSTRAINT ItemsSoldInStorePk PRIMARY KEY (TransactionId),

    -- Validation
    CONSTRAINT CustomerBuyBool CHECK (CustomerBuys IN (0, 1)),

    CONSTRAINT QuantityGtZero CHECK (Quantity > 0),

    CONSTRAINT TradePricePerItemWithTax 
        CHECK (TradePricePerItemWithTax >= TradePricePerItem),

    CONSTRAINT TotalTradePrice 
        CHECK (TotalTradePrice = TradePricePerItem * Quantity),

    CONSTRAINT TotalTradePriceWithTax 
        CHECK (TotalTradePriceWithTax = TradePricePerItemWithTax * Quantity),

    -- FOREGIN KEYS
    CONSTRAINT StoreIdFk FOREIGN KEY (StoreId)
        REFERENCES Store (StoreId),

    CONSTRAINT ItemIdFk FOREIGN KEY (ItemId)
        REFERENCES Item (ItemId),

    CONSTRAINT CustomerIdFk FOREIGN KEY (CustomerId)
        REFERENCES Item (Customer)
);
