CREATE DATABASE finances;

USE finances;

CREATE TABLE Users (
    UserID VARCHAR(255) PRIMARY KEY,
    CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Transactions (
    ID INTEGER PRIMARY KEY AUTO_INCREMENT,
    UserID VARCHAR(255),
    AccountType ENUM('Credit', 'Chequing') NOT NULL,
    Date DATE NOT NULL,
    Description TEXT,
    TransactionType VARCHAR(50),
    Amount DECIMAL(10, 2) NOT NULL,
    Balance DECIMAL(10, 2),
    UNIQUE(Date, Description(100), Amount, TransactionType, UserID, AccountType),
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

CREATE TABLE UserCategories (
    CategoryID INTEGER PRIMARY KEY AUTO_INCREMENT,
    UserID VARCHAR(255),
    CategoryName VARCHAR(50) NOT NULL,
    Description TEXT,
    ColorHex VARCHAR(7),
    UNIQUE(UserID, CategoryName),
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
