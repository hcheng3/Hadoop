Transactions = load '/user/hadoop/inputCSV/transaction.csv' using PigStorage(',') AS ( TransID: int, CustID: int, TransTotal: float, TransNumItems: int, TransDesc: chararray);
Customers = load '/user/hadoop/inputCSV/customer.csv' using PigStorage(',') AS ( ID, Name, Age, CountryCode, Salary);
TranGroup = group Transactions by CustID;
TransInfo = foreach TranGroup generate group as CustID, COUNT(Transactions.TransID)as NumOfTransactions, SUM(Transactions.TransTotal)as TotalSum, MIN(Transactions.TransNumItems) as MiniItems;
CustInfo = foreach Customers generate ID, Name, Salary;
joinCT = join TransInfo by CustID, CustInfo by ID using 'replicated';
result = foreach joinCT generate CustID, Name, Salary,NumOfTransactions,TotalSum,MiniItems;
dump result;
