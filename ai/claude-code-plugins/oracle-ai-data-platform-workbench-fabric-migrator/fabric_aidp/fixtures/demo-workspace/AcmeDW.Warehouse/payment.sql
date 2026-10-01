CREATE TABLE dbo.payment (
    id BIGINT IDENTITY(1,1) NOT NULL,
    amount MONEY,
    paid_on DATE,
    CONSTRAINT pk_payment PRIMARY KEY NONCLUSTERED (id) NOT ENFORCED
)
