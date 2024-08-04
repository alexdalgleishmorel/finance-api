CREATE DATABASE finances;

USE finances;

CREATE TABLE Users (
    UserID VARCHAR(255) PRIMARY KEY
);

CREATE TABLE CreditTransactions (
    ID INTEGER PRIMARY KEY AUTO_INCREMENT,
    UserID VARCHAR(255),
    Date DATE,
    Description TEXT,
    Type VARCHAR(50),
    Amount DECIMAL,
    Category VARCHAR(50),
    UNIQUE(Date, Description(100), Amount, Type, UserID),
    FOREIGN KEY (UserID) REFERENCES Users(UserID)
);

CREATE TABLE ChequingTransactions (
    ID INTEGER PRIMARY KEY AUTO_INCREMENT,
    UserID VARCHAR(255),
    Date DATE,
    Description TEXT,
    Type VARCHAR(50),
    Amount DECIMAL,
    Balance DECIMAL,
    Category VARCHAR(50),
    UNIQUE(Date, Description(100), Amount, Type, UserID),
    FOREIGN KEY (UserID) REFERENCES Users(UserID)
);

CREATE TABLE InvestmentAccounts (
    ID INTEGER PRIMARY KEY AUTO_INCREMENT,
    UserID VARCHAR(255),
    AccountName VARCHAR(100),
    Balance DECIMAL,
    ContributionRoom DECIMAL,
    AverageRateOfReturn DECIMAL,
    UNIQUE(UserID, AccountName),
    FOREIGN KEY (UserID) REFERENCES Users(UserID)
);

CREATE TABLE TransactionCategoryMapping (
    UserID VARCHAR(255),
    TransactionDescription TEXT,
    CategoryName VARCHAR(50),
    PRIMARY KEY (UserID, TransactionDescription(100)),
    FOREIGN KEY (UserID) REFERENCES Users(UserID)
);
