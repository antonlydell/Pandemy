-- SQLite

-- The Database describes General Stores in Runscape and their trading history.
-- This is the test database for pandemy.

-- General Stores in Runescape
CREATE TABLE IF NOT EXISTS Store (
    StoreId   INTEGER,
    StoreName TEXT  NOT NULL,
    Location  TEXT  NOT NULL,
    OwnerId   TEXT  NOT NULL,

    CONSTRAINT StorePk PRIMARY KEY (StoreId),

    CONSTRAINT OwnerIdFk FOREIGN KEY (OwnerId)
        REFERENCES Owner (OwnerId)
);

-- Owners of General Stores
CREATE TABLE IF NOT EXISTS Owner (
    OwnerId   INTEGER, 
    OwnerName TEXT NOT NULL,
    BirthDate TEXT, -- YYYY-MM-DD

    CONSTRAINT OwnerPk PRIMARY KEY (OwnerId),

    CONSTRAINT BirthDateLength CHECK (LENGTH(BirthDate) = 10)
);

-- Customers that have traded in a General Store
CREATE TABLE IF NOT EXISTS Customer (
    CustomerId         INTEGER, 
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
    MemberOnly  INTEGER NOT NULL,
    Description TEXT,

    CONSTRAINT ItemPk PRIMARY KEY (ItemId),

    -- Validation
    CONSTRAINT MemberOnlyBool CHECK (MemberOnly IN (0,1))
);


-- The supply of items in a General Store
CREATE TABLE IF NOT EXISTS StoreSupply (
    StoreSupplyId INTEGER, 
    StoreId       INTEGER NOT NULL,
    ItemId        TEXT    NOT NULL,
    Quantity      INTEGER NOT NULL,
    Price         INTEGER NOT NULL,

    CONSTRAINT StoreSupplyPk PRIMARY KEY (StoreSupplyId),

    -- FOREGIN KEYS
    CONSTRAINT StoreIdFk FOREIGN KEY (StoreId)
        REFERENCES Store (StoreId),

    CONSTRAINT StoreSupplyItemIdFk FOREIGN KEY (ItemId)
        REFERENCES Item (ItemId)

    -- Validation
    CONSTRAINT UniqueSupply UNIQUE(StoreId, ItemId),

    CONSTRAINT QuantityGtZero CHECK (Quantity >= 0),

    CONSTRAINT PriceGtZero CHECK (Price >= 0)
);

-- Items bought by customer and sold by the store to the customer
CREATE TABLE IF NOT EXISTS ItemTradedInStore (
    TransactionId            INTEGER,
    StoreId                  INTEGER NOT NULL,
    ItemId                   INTEGER NOT NULL,
    CustomerId               INTEGER NOT NULL,
    CustomerBuys             INTEGER NOT NULL, -- 1 if the customer buys the item and 0 if he sells
    TransactionTimestamp     TEXT    NOT NULL, -- YYYY-MM-DD HH:MM:SS
    Quantity                 INTEGER NOT NULL,
    TradePricePerItem        REAL    NOT NULL,
    TotalTradePrice          REAL    NOT NULL,

    CONSTRAINT ItemsSoldInStorePk PRIMARY KEY (TransactionId),

    -- FOREGIN KEYS
    CONSTRAINT StoreIdFk FOREIGN KEY (StoreId)
        REFERENCES Store (StoreId),

    CONSTRAINT ItemIdFk FOREIGN KEY (ItemId)
        REFERENCES Item (ItemId),

    CONSTRAINT CustomerIdFk FOREIGN KEY (CustomerId)
        REFERENCES Customer (CustomerId),

    -- Validation
    CONSTRAINT UniqueTransaction UNIQUE (StoreId, ItemId, CustomerId, 
                                         CustomerBuys, TransactionTimestamp, Quantity),
                                         
    CONSTRAINT CustomerBuyBool CHECK (CustomerBuys IN (0, 1)),

    CONSTRAINT QuantityGtZero CHECK (Quantity > 0),

    CONSTRAINT TradePricePerItem 
        CHECK (TradePricePerItem >= 0),

    CONSTRAINT TotalTradePrice 
        CHECK (TotalTradePrice = TradePricePerItem * Quantity),

    CONSTRAINT TransactionTimestampLength 
        CHECK (LENGTH(TransactionTimestamp) = 16)
);
