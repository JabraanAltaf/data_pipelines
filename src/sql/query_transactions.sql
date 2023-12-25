-- Query which checks for insertion of new data into Transactions Table
SELECT * FROM transactions WHERE transactionId = '8c2a0f19-793c-4e66-9e7e-00ea17f6aa6f';

-- Query which checks for updating an existing data field, e.g. amount 
SELECT transactionId, amount FROM transactions WHERE transactionId = '7b3c8eee-3689-4cf8-b874-dfbe515d2eb7';