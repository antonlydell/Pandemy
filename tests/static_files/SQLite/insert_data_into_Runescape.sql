-- SQLite

-- Test data for the Runescape General Store Database.


-- Owner
INSERT INTO Owner (OwnerId, OwnerName, BirthDate)
    VALUES (1, 'Shop keeper', '1982-06-18'),
           (2, 'John General', '1939-09-01'),
           (3, 'Gerhard General', '1945-05-08');

-- Customer
 INSERT INTO Customer (CustomerId, CustomerName, BirthDate, Residence, IsAdventurer)
    VALUES (1, 'Zezima', '1990-07-14', 'Yanille', 1),
           (2, 'Dr Harlow', '1970-01-14', 'Varrock', 0),
           (3, 'Baraek', '1968-12-13', 'Varrock', 0),
           (4, 'Gypsy Aris', '1996-03-24', 'Varrock', 0),
           (5, 'Not a Bot', '2006-05-31', 'Catherby', 1),
           (6, 'Max Pure', '2007-08-20', 'Port Sarim', 1);

-- Store
INSERT INTO Store (StoreId, StoreName, Location, OwnerId)
    VALUES (1, 'Lumbridge General Supplies', 'Lumbridge', 1),
           (2, 'Varrock General Store', 'Varrock', 2),
           (3, 'Falador General Store', 'Falador', 3);

-- Item
INSERT INTO Item (ItemId, ItemName, MemberOnly, Description)
    VALUES (1, 'Pot', 0, 'This pot is empty.'),
           (2, 'Jug',  0, 'This jug is empty.'),
           (3, 'Shears', 0, 'For shearing sheep.'),
           (4, 'Bucket', 0, 'It''s a wooden bucket.'),
           (5, 'Bowl', 0, 'Useful for mixing things.'),
           (6, 'Cake tin', 0, 'Useful for baking things.'),
           (7, 'Tinderbox', 0, 'Useful for lighting a fire.'),
           (8, 'Chisel', 0, 'Good for detailed Crafting.'),
           (9, 'Hammer', 0, 'Good for hitting things.'),
           (10, 'Newcomer map', 0, 'Issued to all new citizens of Gielinor.'),
           (11, 'Unstrung symbol', 0, 'It needs a string so I can wear it.'),
           (12, 'Dragon Scimitar', 1, 'A vicious, curved sword.'),
           (13, 'Amulet of glory', 1, 'A very powerful dragonstone amulet.'),
           (14, 'Ranarr seed', 1, 'A ranarr seed - plant in a herb patch.'),
           (15, 'Swordfish', 0, 'I''d better be careful eating this!'),
           (16, 'Red dragonhide Body', 1, 'Made from 100% real dragonhide.');


-- StoreSupply
INSERT INTO StoreSupply (StoreSupplyId, StoreId, ItemId, Quantity, Price)
    VALUES (1, 2, 1, 5, 1),
           (2, 2, 2, 2, 1),
           (3, 2, 3, 2, 1),
           (4, 2, 4, 3, 2),
           (5, 2, 5, 2, 5),
           (6, 2, 6, 2, 13),
           (7, 2, 7, 2, 1),
           (8, 2, 8, 2, 1),
           (9, 2, 9, 5, 1),
           (10, 2, 10, 5, 1),
           (11, 2, 11, 5, 242),
           (12, 3, 12, 1, 61200),
           (13, 1, 13, 2, 10500),
           (14, 3, 13, 3, 9500),
           (15, 3, 14, 15, 1850),
           (16, 1, 14, 20, 1650);


-- ItemTradedInStore 
INSERT INTO ItemTradedInStore (TransactionId, StoreId, ItemId, CustomerId, CustomerBuys, TransactionTimestamp, 
                               Quantity, TradePricePerItem, TotalTradePrice)
    VALUES (1, 2, 1, 3, 1, '2021-06-18 21:48', 2, 1, 2),
           (2, 3, 3, 3, 1, '2021-06-18 21:49', 1, 1, 1),
           (3, 2, 4, 3, 0, '2021-06-19 20:08', 3, 2, 6),
           (4, 3, 12, 1, 0, '2021-06-26 13:37', 1, 61200, 61200),
           (5, 2, 11, 1, 1, '2007-08-16 13:38', 3, 242, 726),
           (6, 1, 14, 5, 0, '2008-01-01 00:01', 10, 1850, 18500),
           (7, 1, 13, 6, 1, '2009-02-05 22:02', 2, 10500, 21000);
