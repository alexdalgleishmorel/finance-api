CREATE DATABASE finances;

USE finances;

CREATE TABLE Users (
    UserID VARCHAR(255) PRIMARY KEY,
    CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE ChequingTransactions (
    ID INTEGER PRIMARY KEY AUTO_INCREMENT,
    UserID VARCHAR(255),
    Date DATE NOT NULL,
    Description TEXT,
    TransactionType VARCHAR(50),
    Amount DECIMAL(10, 2) NOT NULL,
    Balance DECIMAL(10, 2),
    UNIQUE(Date, Description(100), Amount, TransactionType, UserID),
    FOREIGN KEY (UserID) REFERENCES Users(UserID)
);

CREATE TABLE CreditTransactions (
    ID INTEGER PRIMARY KEY AUTO_INCREMENT,
    UserID VARCHAR(255),
    Date DATE NOT NULL,
    Description TEXT,
    TransactionType VARCHAR(50),
    Amount DECIMAL(10, 2) NOT NULL,
    UNIQUE(Date, Description(100), Amount, TransactionType, UserID),
    FOREIGN KEY (UserID) REFERENCES Users(UserID)
);

CREATE TABLE InvestmentAccounts (
    ID INTEGER PRIMARY KEY AUTO_INCREMENT,
    UserID VARCHAR(255),
    AccountName VARCHAR(100) NOT NULL,
    Balance DECIMAL(10, 2) NOT NULL,
    ContributionRoom DECIMAL(10, 2),
    AverageAnnualContribution DECIMAL(10, 2),
    AverageAnnualRateOfReturn DECIMAL(5, 2),
    UNIQUE(UserID, AccountName),
    FOREIGN KEY (UserID) REFERENCES Users(UserID)
);

CREATE TABLE DefaultCategories (
    CategoryID INT PRIMARY KEY AUTO_INCREMENT,
    AccountType ENUM('Chequing', 'Credit', 'Investment') NOT NULL,
    CategoryName VARCHAR(50) NOT NULL,
    ColorHex VARCHAR(7),
    Description TEXT
);

CREATE TABLE UserCategories (
    CategoryID INTEGER PRIMARY KEY AUTO_INCREMENT,
    UserID VARCHAR(255) DEFAULT NULL,
    AccountType ENUM('Chequing', 'Credit', 'Investment') NOT NULL,
    CategoryName VARCHAR(50) NOT NULL,
    Description TEXT,
    ColorHex VARCHAR(7),
    UNIQUE(UserID, AccountType, CategoryName),
    FOREIGN KEY (UserID) REFERENCES Users(UserID)
);

CREATE TABLE TransactionCategoryMapping (
    UserID VARCHAR(255),
    TransactionDescription TEXT,
    CategoryID INTEGER,
    PRIMARY KEY (UserID, TransactionDescription(100)),
    FOREIGN KEY (UserID) REFERENCES Users(UserID),
    FOREIGN KEY (CategoryID) REFERENCES UserCategories(CategoryID)
);

CREATE TABLE CustomTransactionDescriptionMapping (
    UserID VARCHAR(255),
    OriginalDescription TEXT,
    CustomDescription TEXT,
    PRIMARY KEY (UserID, OriginalDescription(100)),
    FOREIGN KEY (UserID) REFERENCES Users(UserID)
);

CREATE TABLE UploadProgress (
    UserID VARCHAR(255),
    AccountType ENUM('Chequing', 'Credit', 'Investment') NOT NULL,
    Progress DECIMAL(5, 2) DEFAULT NULL,
    PRIMARY KEY (UserID, AccountType),
    FOREIGN KEY (UserID) REFERENCES Users(UserID)
);

INSERT INTO UserCategories (UserID, AccountType, CategoryName, Description, ColorHex)
VALUES
    (NULL, 'Credit', 'Unknown', 'Any transaction that does not fall under any of the other categories', '#CCCCCC'),
    (NULL, 'Credit', 'Shopping', 'Purchases made for clothes, electronics, ecommerce of any sort, etc.', '#FF5733'),
    (NULL, 'Credit', 'Dining Out', 'Expenses for eating out at restaurants or fast food', '#33FF57'),
    (NULL, 'Credit', 'Travel', 'Expenses related to travel and accommodation', '#33A1FF'),
    (NULL, 'Credit', 'Groceries', 'Purchases made at grocery stores', '#FF33F6'),
    (NULL, 'Credit', 'Vehicle', 'Payments for vehicle maintenance or gas', '#FF3333'),
    (NULL, 'Credit', 'Subscriptions', 'Monthly or yearly subscriptions to services', '#3333FF')
;
